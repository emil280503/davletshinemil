from __future__ import annotations

import argparse
import concurrent.futures
import html
import http.client
import math
import pathlib
import re
import shutil
import time
from collections import Counter
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
DEFAULT_TERM_TFIDF_DIR = pathlib.Path("term_tfidf")
DEFAULT_LEMMA_TFIDF_DIR = pathlib.Path("lemma_tfidf")
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


@dataclass
class DocumentStats:
    name: str
    term_counts: Counter[str]
    lemma_counts: Counter[str]
    total_terms: int


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
        description="Скачивает HTML-страницы, строит индекс, считает TF-IDF и выполняет булев и векторный поиск."
    )
    parser.add_argument("--urls", type=pathlib.Path, default=DEFAULT_URLS_FILE)
    parser.add_argument("--output", type=pathlib.Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--index", type=pathlib.Path, default=DEFAULT_INDEX_FILE)
    parser.add_argument("--tokens", type=pathlib.Path, default=DEFAULT_TOKENS_FILE)
    parser.add_argument("--lemmas", type=pathlib.Path, default=DEFAULT_LEMMAS_FILE)
    parser.add_argument("--inverted-index", type=pathlib.Path, default=DEFAULT_INVERTED_INDEX_FILE)
    parser.add_argument("--term-tfidf-dir", type=pathlib.Path, default=DEFAULT_TERM_TFIDF_DIR)
    parser.add_argument("--lemma-tfidf-dir", type=pathlib.Path, default=DEFAULT_LEMMA_TFIDF_DIR)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--query", type=str, help="Булев запрос с операторами AND, OR, NOT и круглыми скобками.")
    parser.add_argument("--vector-query", type=str, help="Строка запроса для векторного поиска по TF-IDF.")
    parser.add_argument("--top-k", type=int, default=10, help="Сколько лучших документов вернуть в векторном поиске.")
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


def collect_corpus_data(
    input_dir: pathlib.Path,
) -> tuple[
    list[str],
    dict[str, list[str]],
    dict[str, list[str]],
    dict[str, str],
    list[DocumentStats],
    dict[str, float],
    dict[str, float],
]:
    unique_tokens: set[str] = set()
    lemma_to_tokens: dict[str, set[str]] = {}
    inverted_index: dict[str, set[str]] = {}
    term_document_frequency: Counter[str] = Counter()
    lemma_document_frequency: Counter[str] = Counter()
    doc_to_url = load_document_urls(input_dir / "index.txt")
    document_stats: list[DocumentStats] = []

    for page_path in iter_page_files(input_dir):
        document_name = page_path.name
        visible_text = extract_visible_text(page_path.read_text(encoding="utf-8"))
        term_counts: Counter[str] = Counter()
        lemma_counts: Counter[str] = Counter()

        for raw_token in TOKEN_PATTERN.findall(visible_text.lower()):
            lemma = normalize_term(raw_token)
            if lemma is None:
                continue

            unique_tokens.add(raw_token)
            lemma_to_tokens.setdefault(lemma, set()).add(raw_token)
            term_counts[raw_token] += 1
            lemma_counts[lemma] += 1

        total_terms = sum(term_counts.values())
        document_stats.append(
            DocumentStats(
                name=document_name,
                term_counts=term_counts,
                lemma_counts=lemma_counts,
                total_terms=total_terms,
            )
        )

        for term in term_counts:
            term_document_frequency[term] += 1
        for lemma in lemma_counts:
            lemma_document_frequency[lemma] += 1
            inverted_index.setdefault(lemma, set()).add(document_name)

        doc_to_url.setdefault(document_name, "")

    total_documents = len(document_stats)
    term_idf = {
        term: math.log(total_documents / frequency)
        for term, frequency in sorted(term_document_frequency.items(), key=lambda item: item[0])
    }
    lemma_idf = {
        lemma: math.log(total_documents / frequency)
        for lemma, frequency in sorted(lemma_document_frequency.items(), key=lambda item: item[0])
    }
    sorted_tokens = sorted(unique_tokens)
    sorted_lemmas = {
        lemma: sorted(tokens)
        for lemma, tokens in sorted(lemma_to_tokens.items(), key=lambda item: item[0])
    }
    sorted_inverted_index = {
        lemma: sorted(documents)
        for lemma, documents in sorted(inverted_index.items(), key=lambda item: item[0])
    }
    sorted_doc_to_url = dict(sorted(doc_to_url.items(), key=lambda item: item[0]))
    return (
        sorted_tokens,
        sorted_lemmas,
        sorted_inverted_index,
        sorted_doc_to_url,
        sorted(document_stats, key=lambda item: item.name),
        term_idf,
        lemma_idf,
    )


def write_tokens(tokens: list[str], output_path: pathlib.Path) -> None:
    output_path.write_text("\n".join(tokens) + "\n", encoding="utf-8")


def write_lemmas(lemma_to_tokens: dict[str, list[str]], output_path: pathlib.Path) -> None:
    lines = [f"{lemma} {' '.join(tokens)}" for lemma, tokens in lemma_to_tokens.items()]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_inverted_index(inverted_index: dict[str, list[str]], output_path: pathlib.Path) -> None:
    lines = [f"{term} {' '.join(documents)}" for term, documents in inverted_index.items()]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def reset_output_dir(output_dir: pathlib.Path) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)


def write_document_tfidf(
    document_stats: list[DocumentStats],
    term_idf: dict[str, float],
    lemma_idf: dict[str, float],
    term_output_dir: pathlib.Path,
    lemma_output_dir: pathlib.Path,
) -> None:
    reset_output_dir(term_output_dir)
    reset_output_dir(lemma_output_dir)

    for document in document_stats:
        term_lines: list[str] = []
        lemma_lines: list[str] = []

        if document.total_terms > 0:
            for term in sorted(document.term_counts):
                tf = document.term_counts[term] / document.total_terms
                idf = term_idf[term]
                term_lines.append(f"{term} {idf:.6f} {tf * idf:.6f}")

            for lemma in sorted(document.lemma_counts):
                tf = document.lemma_counts[lemma] / document.total_terms
                idf = lemma_idf[lemma]
                lemma_lines.append(f"{lemma} {idf:.6f} {tf * idf:.6f}")

        (term_output_dir / f"{pathlib.Path(document.name).stem}.txt").write_text(
            "\n".join(term_lines) + ("\n" if term_lines else ""),
            encoding="utf-8",
        )
        (lemma_output_dir / f"{pathlib.Path(document.name).stem}.txt").write_text(
            "\n".join(lemma_lines) + ("\n" if lemma_lines else ""),
            encoding="utf-8",
        )


def build_lemma_document_vectors(
    document_stats: list[DocumentStats],
    lemma_idf: dict[str, float],
) -> dict[str, dict[str, float]]:
    vectors: dict[str, dict[str, float]] = {}
    for document in document_stats:
        vector: dict[str, float] = {}
        if document.total_terms > 0:
            for lemma, count in document.lemma_counts.items():
                tf = count / document.total_terms
                vector[lemma] = tf * lemma_idf[lemma]
        vectors[document.name] = vector
    return vectors


def build_query_vector(query: str, lemma_idf: dict[str, float]) -> dict[str, float]:
    lemma_counts: Counter[str] = Counter()
    for raw_token in TOKEN_PATTERN.findall(query.lower()):
        lemma = normalize_term(raw_token)
        if lemma is not None and lemma in lemma_idf:
            lemma_counts[lemma] += 1

    total_terms = sum(lemma_counts.values())
    if total_terms == 0:
        return {}

    return {
        lemma: (count / total_terms) * lemma_idf[lemma]
        for lemma, count in lemma_counts.items()
    }


def vector_norm(vector: dict[str, float]) -> float:
    return math.sqrt(sum(weight * weight for weight in vector.values()))


def cosine_similarity(left: dict[str, float], right: dict[str, float]) -> float:
    if not left or not right:
        return 0.0

    left_norm = vector_norm(left)
    right_norm = vector_norm(right)
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0

    shared_terms = set(left) & set(right)
    dot_product = sum(left[term] * right[term] for term in shared_terms)
    return dot_product / (left_norm * right_norm)


def run_vector_search(
    query: str,
    document_vectors: dict[str, dict[str, float]],
    lemma_idf: dict[str, float],
    document_urls: dict[str, str],
    top_k: int,
) -> list[tuple[str, str, float]]:
    query_vector = build_query_vector(query, lemma_idf)
    scored_documents: list[tuple[str, str, float]] = []

    for document_name, document_vector in document_vectors.items():
        score = cosine_similarity(query_vector, document_vector)
        if score > 0.0:
            scored_documents.append((document_name, document_urls.get(document_name, ""), score))

    scored_documents.sort(key=lambda item: (-item[2], item[0]))
    return scored_documents[:top_k]


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
            lemma = normalize_term(token)
            stack.append(set(inverted_index.get(lemma, [])) if lemma else set())

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

    (
        tokens,
        lemma_to_tokens,
        inverted_index,
        document_urls,
        document_stats,
        term_idf,
        lemma_idf,
    ) = collect_corpus_data(args.output)

    write_tokens(tokens, args.tokens)
    write_lemmas(lemma_to_tokens, args.lemmas)
    write_inverted_index(inverted_index, args.inverted_index)
    write_document_tfidf(
        document_stats=document_stats,
        term_idf=term_idf,
        lemma_idf=lemma_idf,
        term_output_dir=args.term_tfidf_dir,
        lemma_output_dir=args.lemma_tfidf_dir,
    )

    print(f"Обработано HTML-файлов: {len(page_files)}")
    print(f"Токенов: {len(tokens)}")
    print(f"Лемм: {len(lemma_to_tokens)}")
    print(f"Терминов в инвертированном индексе: {len(inverted_index)}")
    print(f"Файл токенов: {args.tokens.resolve()}")
    print(f"Файл лемм: {args.lemmas.resolve()}")
    print(f"Файл инвертированного индекса: {args.inverted_index.resolve()}")
    print(f"Каталог TF-IDF по терминам: {args.term_tfidf_dir.resolve()}")
    print(f"Каталог TF-IDF по леммам: {args.lemma_tfidf_dir.resolve()}")

    if args.query:
        results = run_boolean_search(args.query, inverted_index, document_urls)
        print(f"Булев запрос: {args.query}")
        print(f"Найдено документов: {len(results)}")
        for document_name, url in results:
            print(f"{document_name}\t{url}")

    if args.vector_query:
        document_vectors = build_lemma_document_vectors(document_stats, lemma_idf)
        results = run_vector_search(
            query=args.vector_query,
            document_vectors=document_vectors,
            lemma_idf=lemma_idf,
            document_urls=document_urls,
            top_k=max(args.top_k, 1),
        )
        print(f"Векторный запрос: {args.vector_query}")
        print(f"Найдено документов: {len(results)}")
        for document_name, url, score in results:
            print(f"{score:.6f}\t{document_name}\t{url}")


if __name__ == "__main__":
    main()
