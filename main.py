from __future__ import annotations

import argparse
import collections
import pathlib
import re
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Iterable
from urllib import error, parse, request


DEFAULT_SEEDS = [
    "https://ru.wikipedia.org/wiki/Россия",
    "https://ru.wikipedia.org/wiki/Москва",
    "https://ru.wikipedia.org/wiki/Санкт-Петербург",
    "https://ru.wikipedia.org/wiki/Литература",
    "https://ru.wikipedia.org/wiki/Наука",
]

DEFAULT_ALLOWED_DOMAINS = ["ru.wikipedia.org"]
DEFAULT_FORBIDDEN_EXTENSIONS = {
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
class CrawlResult:
    url: str
    file_name: str


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for attr_name, attr_value in attrs:
            if attr_name.lower() == "href" and attr_value:
                self.links.append(attr_value)


class WebCrawler:
    def __init__(
        self,
        seeds: Iterable[str],
        output_dir: pathlib.Path,
        limit: int,
        allowed_domains: Iterable[str],
        delay: float,
        timeout: float,
    ) -> None:
        self.queue = collections.deque(seeds)
        self.output_dir = output_dir
        self.limit = limit
        self.allowed_domains = {domain.lower() for domain in allowed_domains}
        self.delay = delay
        self.timeout = timeout
        self.visited: set[str] = set()
        self.saved: list[CrawlResult] = []

        self.opener = request.build_opener()
        self.opener.addheaders = [
            (
                "User-Agent",
                "Mozilla/5.0 (compatible; PythonProject2Crawler/1.0; +https://example.com/bot)",
            )
        ]

    def crawl(self) -> list[CrawlResult]:
        self.output_dir.mkdir(parents=True, exist_ok=True)

        while self.queue and len(self.saved) < self.limit:
            current_url = self.queue.popleft()
            normalized_url = self.normalize_url(current_url)
            if not normalized_url or normalized_url in self.visited:
                continue

            self.visited.add(normalized_url)
            try:
                html = self.fetch_html(normalized_url)
            except (error.URLError, error.HTTPError, TimeoutError, ValueError):
                continue

            file_name = f"{len(self.saved) + 1:03}.html"
            (self.output_dir / file_name).write_text(html, encoding="utf-8")
            self.saved.append(CrawlResult(url=normalized_url, file_name=file_name))

            for link in self.extract_links(html, normalized_url):
                if link not in self.visited:
                    self.queue.append(link)

            if self.delay > 0:
                time.sleep(self.delay)

        self.write_index()
        return self.saved

    def fetch_html(self, url: str) -> str:
        with self.opener.open(url, timeout=self.timeout) as response:
            content_type = response.headers.get_content_type()
            if content_type != "text/html":
                raise ValueError(f"Unsupported content type: {content_type}")

            raw_data = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
            return raw_data.decode(charset, errors="replace")

    def extract_links(self, html: str, base_url: str) -> list[str]:
        parser = LinkExtractor()
        parser.feed(html)
        normalized_links: list[str] = []
        for link in parser.links:
            normalized = self.normalize_url(parse.urljoin(base_url, link))
            if normalized:
                normalized_links.append(normalized)
        return normalized_links

    def normalize_url(self, url: str) -> str | None:
        parsed = parse.urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return None

        domain = parsed.netloc.lower()
        if self.allowed_domains and domain not in self.allowed_domains:
            return None

        if any(parsed.path.lower().endswith(ext) for ext in DEFAULT_FORBIDDEN_EXTENSIONS):
            return None

        if re.search(r":(?!//)", parsed.path):
            return None

        encoded_path = parse.quote(parse.unquote(parsed.path), safe="/:@")
        cleaned = parsed._replace(path=encoded_path, fragment="", query="")
        return parse.urlunparse(cleaned)

    def write_index(self) -> None:
        index_path = self.output_dir / "index.txt"
        lines = [f"{result.file_name}\t{result.url}" for result in self.saved]
        index_path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Скачивает HTML-страницы и сохраняет их вместе с index.txt."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Сколько HTML-страниц сохранить. По умолчанию: 100.",
    )
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        default=pathlib.Path("downloaded_pages"),
        help="Каталог для сохранения страниц и index.txt.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Задержка между запросами в секундах.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Таймаут одного HTTP-запроса в секундах.",
    )
    parser.add_argument(
        "--seed",
        action="append",
        dest="seeds",
        help="Стартовый URL. Можно передавать несколько раз.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seeds = args.seeds or DEFAULT_SEEDS

    crawler = WebCrawler(
        seeds=seeds,
        output_dir=args.output,
        limit=args.limit,
        allowed_domains=DEFAULT_ALLOWED_DOMAINS,
        delay=args.delay,
        timeout=args.timeout,
    )
    results = crawler.crawl()
    print(f"Сохранено страниц: {len(results)}")
    print(f"Каталог выгрузки: {args.output.resolve()}")


if __name__ == "__main__":
    main()
