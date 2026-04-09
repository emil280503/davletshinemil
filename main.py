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
IGNORED_TAGS = {"script", "style", "noscript", "svg"}
EXCLUDED_POS = {"CONJ", "PREP", "PRCL", "INTJ"}
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
        description="Скачивает HTML-страницы и строит списки токенов и лемм по сохраненным документам."
    )
    parser.add_argument(
        "--urls",
        type=pathlib.Path,
        default=DEFAULT_URLS_FILE,
        help="Путь к файлу со списком URL, по одному адресу в строке.",
    )
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Каталог для сохранения HTML-файлов.",
    )
    parser.add_argument(
        "--index",
        type=pathlib.Path,
        default=DEFAULT_INDEX_FILE,
        help="Файл индекса в формате 'номер файла TAB ссылка'.",
    )
    parser.add_argument(
        "--tokens",
        type=pathlib.Path,
        default=DEFAULT_TOKENS_FILE,
        help="Файл для списка уникальных токенов.",
    )
    parser.add_argument(
        "--lemmas",
        type=pathlib.Path,
        default=DEFAULT_LEMMAS_FILE,
        help="Файл для списка лемм и соответствующих им токенов.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="Таймаут одного HTTP-запроса в секундах.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Сколько раз повторять скачивание одной страницы при временной ошибке.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Количество параллельных загрузок.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Не скачивать страницы заново, а обработать уже сохраненные HTML-файлы.",
    )
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


def write_index(results: list[DownloadResult], index_path: pathlib.Path) -> None:
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


def should_keep_token(token: str) -> tuple[bool, str | None]:
    if not is_valid_token(token):
        return False, None

    parse_result = MORPH.parse(token)[0]
    if parse_result.tag.POS in EXCLUDED_POS:
        return False, None

    lemma = parse_result.normal_form
    if not is_valid_token(lemma):
        return False, None

    return True, lemma


def collect_tokens_and_lemmas(input_dir: pathlib.Path) -> tuple[list[str], dict[str, list[str]]]:
    unique_tokens: set[str] = set()
    lemma_to_tokens: dict[str, set[str]] = {}

    for page_path in iter_page_files(input_dir):
        visible_text = extract_visible_text(page_path.read_text(encoding="utf-8"))
        for raw_token in TOKEN_PATTERN.findall(visible_text.lower()):
            keep_token, lemma = should_keep_token(raw_token)
            if not keep_token or lemma is None:
                continue

            unique_tokens.add(raw_token)
            lemma_to_tokens.setdefault(lemma, set()).add(raw_token)

    sorted_tokens = sorted(unique_tokens)
    sorted_lemmas = {
        lemma: sorted(tokens)
        for lemma, tokens in sorted(lemma_to_tokens.items(), key=lambda item: item[0])
    }
    return sorted_tokens, sorted_lemmas


def write_tokens(tokens: list[str], output_path: pathlib.Path) -> None:
    output_path.write_text("\n".join(tokens) + "\n", encoding="utf-8")


def write_lemmas(lemma_to_tokens: dict[str, list[str]], output_path: pathlib.Path) -> None:
    lines = [f"{lemma} {' '.join(tokens)}" for lemma, tokens in lemma_to_tokens.items()]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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

    write_index(results, args.output / "index.txt")
    write_index(results, args.index)


def main() -> None:
    args = parse_args()

    if not args.skip_download:
        download_pages(args)
        print(f"Каталог выгрузки: {args.output.resolve()}")
        print(f"Файл индекса: {args.index.resolve()}")

    page_files = iter_page_files(args.output)
    if not page_files:
        raise FileNotFoundError(f"В каталоге {args.output} нет HTML-файлов для обработки.")

    tokens, lemma_to_tokens = collect_tokens_and_lemmas(args.output)
    write_tokens(tokens, args.tokens)
    write_lemmas(lemma_to_tokens, args.lemmas)

    print(f"Обработано HTML-файлов: {len(page_files)}")
    print(f"Токенов: {len(tokens)}")
    print(f"Лемм: {len(lemma_to_tokens)}")
    print(f"Файл токенов: {args.tokens.resolve()}")
    print(f"Файл лемм: {args.lemmas.resolve()}")


if __name__ == "__main__":
    main()
