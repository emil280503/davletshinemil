from __future__ import annotations

import argparse
import concurrent.futures
import http.client
import pathlib
import shutil
import time
from dataclasses import dataclass
from urllib import error, parse, request


DEFAULT_URLS_FILE = pathlib.Path("urls.txt")
DEFAULT_OUTPUT_DIR = pathlib.Path("downloaded_pages")
DEFAULT_INDEX_FILE = pathlib.Path("index.txt")
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


@dataclass
class DownloadResult:
    url: str
    file_name: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Скачивает HTML-страницы из заранее подготовленного списка URL."
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


def fetch_html(
    url: str,
    timeout: float,
    retries: int,
) -> str:
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


def main() -> None:
    args = parse_args()
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
        html = downloaded_html.get(position)
        if html is None:
            continue

        file_name = f"{len(results) + 1:03}.html"
        (args.output / file_name).write_text(html, encoding="utf-8")
        results.append(DownloadResult(url=url, file_name=file_name))
        if len(results) >= 100:
            break

    if len(results) < 100:
        raise RuntimeError(f"Удалось скачать только {len(results)} страниц из {len(urls)} URL.")

    write_index(results, args.output / "index.txt")
    write_index(results, args.index)
    print(f"Сохранено страниц: {len(results)}")
    print(f"Каталог выгрузки: {args.output.resolve()}")
    print(f"Файл индекса: {args.index.resolve()}")


if __name__ == "__main__":
    main()
