from __future__ import annotations

import argparse
import concurrent.futures
import html
import http.client
import pathlib
import re
import shutil
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib import error, parse, request

import pymorphy3


DEFAULT_URLS_FILE = pathlib.Path("urls.txt")
DEFAULT_OUTPUT_DIR = pathlib.Path("downloaded_pages")
DEFAULT_INDEX_FILE = pathlib.Path("index.txt")
DEFAULT_TOKENS_FILE = pathlib.Path("tokens.txt")
DEFAULT_LEMMAS_FILE = pathlib.Path("lemmas.txt")
DEFAULT_INVERTED_INDEX_FILE = pathlib.Path("inverted_index.txt")
FORBIDDEN_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
    ".webp",
    ".css",
    ".js",
    ".json",
    ".pdf",
    ".zip",
    ".xml",
    ".ico",
    ".mp3",
    ".mp4",
    ".avi",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
}
TOKEN_PATTERN = re.compile(r"[а-яё]+(?:-[а-яё]+)*", re.IGNORECASE)
QUERY_TOKEN_PATTERN = re.compile(r"\(|\)|AND|OR|NOT|[а-яё]+(?:-[а-яё]+)*", re.IGNORECASE)
IGNORED_TAGS = {"script", "style", "noscript", "svg"}
EXCLUDED_POS = {"CONJ", "PREP", "PRCL", "INTJ"}
OPERATOR_PRIORITY = {"NOT": 3, "AND": 2, "OR": 1}
STOPWORDS = {
    "а",
    "без",
    "более",
    "бы",
    "был",
    "была",
    "были",
    "было",
    "быть",
    "в",
    "вам",
    "вас",
    "весь",
    "во",
    "вот",
    "все",
    "всего",
    "всех",
    "вы",
    "где",
    "да",
    "даже",
    "для",
    "до",
    "его",
    "ее",
    "ей",
    "если",
    "есть",
    "еще",
    "же",
    "за",
    "здесь",
    "и",
    "из",
    "или",
    "им",
    "их",
    "к",
    "как",
    "ко",
    "когда",
    "кто",
    "ли",
    "либо",
    "между",
    "меня",
    "мне",
    "может",
    "мы",
    "на",
    "над",
    "надо",
    "наш",
    "не",
    "него",
    "нее",
    "нет",
    "ни",
    "них",
    "но",
    "ну",
    "о",
    "об",
    "однако",
    "он",
    "она",
    "они",
    "оно",
    "от",
    "очень",
    "по",
    "под",
    "при",
    "с",
    "со",
    "так",
    "также",
    "такой",
    "там",
    "те",
    "тем",
    "то",
    "того",
    "тоже",
    "той",
    "только",
    "том",
    "ты",
    "у",
    "уж",
    "уже",
    "хотя",
    "чего",
    "чей",
    "чем",
    "через",
    "что",
    "чтобы",
    "чье",
    "чья",
    "эта",
    "эти",
    "это",
    "я",
}
MORPH = pymorphy3.MorphAnalyzer()


@dataclass
class DownloadResult:
    url: str
    file_name: str


class VisibleTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._ignored_stack: list[str] = []
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in IGNORED_TAGS:
            self._ignored_stack.append(tag)

    def handle_endtag(self, tag: str) -> None:
        if self._ignored_stack and self._ignored_stack[-1] == tag:
            self._ignored_stack.pop()

    def handle_data(self, data: str) -> None:
        if self._ignored_stack:
            return
        cleaned = html.unescape(data).strip()
        if cleaned:
            self._chunks.append(cleaned)

    def get_text(self) -> str:
        return "\n".join(self._chunks)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Скачивает HTML-страницы, строит токены, леммы, инвертированный индекс и выполняет булев поиск."
    )
    parser.add_argument("--urls", type=pathlib.Path, default=DEFAULT_URLS_FILE)
    parser.add_argument("--output", type=pathlib.Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--index", type=pathlib.Path, default=DEFAULT_INDEX_FILE)
    parser.add_argument("--tokens", type=pathlib.Path, default=DEFAULT_TOKENS_FILE)
    parser.add_argument("--lemmas", type=pathlib.Path, default=DEFAULT_LEMMAS_FILE)
    parser.add_argument("--inverted-index", type=pathlib.Path, default=DEFAULT_INVERTED_INDEX_FILE)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--query", type=str, help="Булев запрос с операторами AND, OR, NOT и круглыми скобками.")
    return parser.parse_args()


def normalize_url(url: str) -> str | None:
    stripped = url.strip()
    if not stripped or stripped.startswith("#"):
        return None

    parsed = parse.urlparse(stripped)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None

    if any(parsed.path.lower().endswith(ext) for ext in FORBIDDEN_EXTENSIONS):
        return None

    encoded_path = parse.quote(parse.unquote(parsed.path), safe="/:@")
    cleaned = parsed._replace(path=encoded_path, fragment="")
    return parse.urlunparse(cleaned)


def load_urls(urls_file: pathlib.Path) -> list[str]:
    seen: set[str] = set()
    urls: list[str] = []

    for line in urls_file.read_text(encoding="utf-8").splitlines():
        normalized = normalize_url(line)
        if normalized and normalized not in seen:
            seen.add(normalized)
            urls.append(normalized)

    if len(urls) < 100:
        raise ValueError(f"В файле {urls_file} найдено меньше 100 корректных URL: {len(urls)}")

    return urls


def fetch_html(url: str, timeout: float, retries: int) -> str:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            opener = request.build_opener()
            opener.addheaders = [
                (
                    "User-Agent",
                    "Mozilla/5.0 (compatible; PythonProject2Crawler/2.0; +https://example.com/bot)",
                )
            ]
            with opener.open(url, timeout=timeout) as response:
                content_type = response.headers.get_content_type()
                if content_type != "text/html":
                    raise ValueError(f"Unsupported content type: {content_type}")

                raw_data = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
                return raw_data.decode(charset, errors="replace")
        except (
            error.URLError,
            error.HTTPError,
            TimeoutError,
            ValueError,
            ConnectionError,
            http.client.HTTPException,
        ) as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(1)

    assert last_error is not None
    raise last_error


def write_download_index(results: list[DownloadResult], index_path: pathlib.Path) -> None:
    lines = [f"{result.file_name}\t{result.url}" for result in results]
    index_path.write_text("\n".join(lines), encoding="utf-8")


def extract_visible_text(html_content: str) -> str:
    extractor = VisibleTextExtractor()
    extractor.feed(html_content)
    extractor.close()
    return extractor.get_text()


def iter_page_files(input_dir: pathlib.Path) -> list[pathlib.Path]:
    return sorted(path for path in input_dir.glob("*.html") if path.is_file())


def is_valid_token(token: str) -> bool:
    if not token or any(char.isdigit() for char in token):
        return False
    if token in STOPWORDS:
        return False
    if not TOKEN_PATTERN.fullmatch(token):
        return False
    if "--" in token or token.startswith("-") or token.endswith("-"):
        return False
    return True


def normalize_term(token: str) -> str | None:
    token = token.lower()
    if not is_valid_token(token):
        return None

    parse_result = MORPH.parse(token)[0]
    if parse_result.tag.POS in EXCLUDED_POS:
        return None

    lemma = parse_result.normal_form
    if not is_valid_token(lemma):
        return None
    return lemma


def collect_corpus_data(
    input_dir: pathlib.Path,
) -> tuple[list[str], dict[str, list[str]], dict[str, list[str]], dict[str, str]]:
    unique_tokens: set[str] = set()
    lemma_to_tokens: dict[str, set[str]] = {}
    inverted_index: dict[str, set[str]] = {}
    doc_to_url = load_document_urls(input_dir / "index.txt")

    for page_path in iter_page_files(input_dir):
        document_name = page_path.name
        visible_text = extract_visible_text(page_path.read_text(encoding="utf-8"))
        document_terms: set[str] = set()

        for raw_token in TOKEN_PATTERN.findall(visible_text.lower()):
            lemma = normalize_term(raw_token)
            if lemma is None:
                continue

            unique_tokens.add(raw_token)
            lemma_to_tokens.setdefault(lemma, set()).add(raw_token)
            document_terms.add(lemma)

        for term in document_terms:
            inverted_index.setdefault(term, set()).add(document_name)

        doc_to_url.setdefault(document_name, "")

    sorted_tokens = sorted(unique_tokens)
    sorted_lemmas = {
        lemma: sorted(tokens)
        for lemma, tokens in sorted(lemma_to_tokens.items(), key=lambda item: item[0])
    }
    sorted_inverted_index = {
        term: sorted(documents)
        for term, documents in sorted(inverted_index.items(), key=lambda item: item[0])
    }
    sorted_doc_to_url = dict(sorted(doc_to_url.items(), key=lambda item: item[0]))
    return sorted_tokens, sorted_lemmas, sorted_inverted_index, sorted_doc_to_url


def write_tokens(tokens: list[str], output_path: pathlib.Path) -> None:
    output_path.write_text("\n".join(tokens) + "\n", encoding="utf-8")


def write_lemmas(lemma_to_tokens: dict[str, list[str]], output_path: pathlib.Path) -> None:
    lines = [f"{lemma} {' '.join(tokens)}" for lemma, tokens in lemma_to_tokens.items()]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_inverted_index(inverted_index: dict[str, list[str]], output_path: pathlib.Path) -> None:
    lines = [f"{term} {' '.join(documents)}" for term, documents in inverted_index.items()]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def load_document_urls(index_path: pathlib.Path) -> dict[str, str]:
    if not index_path.exists():
        return {}

    document_urls: dict[str, str] = {}
    for line in index_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        parts = line.split("\t", maxsplit=1)
        if len(parts) == 2:
            document_urls[parts[0]] = parts[1]
    return document_urls


def tokenize_query(query: str) -> list[str]:
    tokens = QUERY_TOKEN_PATTERN.findall(query)
    if not tokens:
        raise ValueError("Пустой или некорректный запрос.")
    return [token.upper() if token.upper() in OPERATOR_PRIORITY else token for token in tokens]


def to_postfix(query_tokens: list[str]) -> list[str]:
    output: list[str] = []
    operators: list[str] = []

    for token in query_tokens:
        if token == "(":
            operators.append(token)
        elif token == ")":
            while operators and operators[-1] != "(":
                output.append(operators.pop())
            if not operators:
                raise ValueError("Несогласованные скобки в запросе.")
            operators.pop()
        elif token in OPERATOR_PRIORITY:
            while (
                operators
                and operators[-1] in OPERATOR_PRIORITY
                and (
                    OPERATOR_PRIORITY[operators[-1]] > OPERATOR_PRIORITY[token]
                    or (
                        OPERATOR_PRIORITY[operators[-1]] == OPERATOR_PRIORITY[token]
                        and token != "NOT"
                    )
                )
            ):
                output.append(operators.pop())
            operators.append(token)
        else:
            output.append(token)

    while operators:
        operator = operators.pop()
        if operator == "(":
            raise ValueError("Несогласованные скобки в запросе.")
        output.append(operator)

    return output


def evaluate_postfix(postfix_tokens: list[str], inverted_index: dict[str, list[str]], all_documents: set[str]) -> set[str]:
    stack: list[set[str]] = []

    for token in postfix_tokens:
        if token == "NOT":
            if not stack:
                raise ValueError("Оператор NOT не имеет аргумента.")
            operand = stack.pop()
            stack.append(all_documents - operand)
        elif token in {"AND", "OR"}:
            if len(stack) < 2:
                raise ValueError(f"Оператор {token} не имеет достаточного числа аргументов.")
            right = stack.pop()
            left = stack.pop()
            stack.append(left & right if token == "AND" else left | right)
        else:
            term = normalize_term(token)
            stack.append(set(inverted_index.get(term, [])) if term else set())

    if len(stack) != 1:
        raise ValueError("Некорректная структура булевого запроса.")
    return stack[0]


def run_boolean_search(
    query: str,
    inverted_index: dict[str, list[str]],
    document_urls: dict[str, str],
) -> list[tuple[str, str]]:
    postfix_tokens = to_postfix(tokenize_query(query))
    matched_documents = evaluate_postfix(postfix_tokens, inverted_index, set(document_urls))
    return [(document_name, document_urls.get(document_name, "")) for document_name in sorted(matched_documents)]


def download_pages(args: argparse.Namespace) -> None:
    urls = load_urls(args.urls)

    if args.output.exists():
        shutil.rmtree(args.output)
    args.output.mkdir(parents=True, exist_ok=True)

    downloaded_html: dict[int, str] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(fetch_html, url, args.timeout, args.retries): (position, url)
            for position, url in enumerate(urls)
        }
        for future in concurrent.futures.as_completed(futures):
            position, _ = futures[future]
            try:
                downloaded_html[position] = future.result()
            except Exception:
                continue

    results: list[DownloadResult] = []
    for position, url in enumerate(urls):
        html_content = downloaded_html.get(position)
        if html_content is None:
            continue

        file_name = f"{len(results) + 1:03}.html"
        (args.output / file_name).write_text(html_content, encoding="utf-8")
        results.append(DownloadResult(url=url, file_name=file_name))
        if len(results) >= 100:
            break

    if len(results) < 100:
        raise RuntimeError(f"Удалось скачать только {len(results)} страниц из {len(urls)} URL.")

    write_download_index(results, args.output / "index.txt")
    write_download_index(results, args.index)


def main() -> None:
    args = parse_args()

    if not args.skip_download:
        download_pages(args)
        print(f"Каталог выгрузки: {args.output.resolve()}")
        print(f"Файл индекса URL: {args.index.resolve()}")

    page_files = iter_page_files(args.output)
    if not page_files:
        raise FileNotFoundError(f"В каталоге {args.output} нет HTML-файлов для обработки.")

    tokens, lemma_to_tokens, inverted_index, document_urls = collect_corpus_data(args.output)
    write_tokens(tokens, args.tokens)
    write_lemmas(lemma_to_tokens, args.lemmas)
    write_inverted_index(inverted_index, args.inverted_index)

    print(f"Обработано HTML-файлов: {len(page_files)}")
    print(f"Токенов: {len(tokens)}")
    print(f"Лемм: {len(lemma_to_tokens)}")
    print(f"Терминов в инвертированном индексе: {len(inverted_index)}")
    print(f"Файл токенов: {args.tokens.resolve()}")
    print(f"Файл лемм: {args.lemmas.resolve()}")
    print(f"Файл инвертированного индекса: {args.inverted_index.resolve()}")

    if args.query:
        results = run_boolean_search(args.query, inverted_index, document_urls)
        print(f"Запрос: {args.query}")
        print(f"Найдено документов: {len(results)}")
        for document_name, url in results:
            print(f"{document_name}\t{url}")


if __name__ == "__main__":
    main()
