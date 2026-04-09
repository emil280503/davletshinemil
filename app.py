from __future__ import annotations

import pathlib
from dataclasses import dataclass

from flask import Flask, render_template, request

from main import (
    DEFAULT_INDEX_FILE,
    DEFAULT_LEMMA_TFIDF_DIR,
    DEFAULT_OUTPUT_DIR,
    build_lemma_document_vectors,
    collect_corpus_data,
    load_document_urls,
    run_vector_search,
)


@dataclass
class SearchEngine:
    document_urls: dict[str, str]
    document_vectors: dict[str, dict[str, float]]
    lemma_idf: dict[str, float]


def load_search_engine_from_tfidf(
    tfidf_dir: pathlib.Path,
    index_path: pathlib.Path,
) -> SearchEngine | None:
    if not tfidf_dir.exists():
        return None

    document_vectors: dict[str, dict[str, float]] = {}
    lemma_idf: dict[str, float] = {}

    for tfidf_file in sorted(tfidf_dir.glob("*.txt")):
        vector: dict[str, float] = {}
        for line in tfidf_file.read_text(encoding="utf-8").splitlines():
            parts = line.split()
            if len(parts) != 3:
                continue

            lemma, idf_raw, tfidf_raw = parts
            idf = float(idf_raw)
            tfidf = float(tfidf_raw)
            vector[lemma] = tfidf
            lemma_idf.setdefault(lemma, idf)

        document_vectors[f"{tfidf_file.stem}.html"] = vector

    if not document_vectors:
        return None

    return SearchEngine(
        document_urls=load_document_urls(index_path),
        document_vectors=document_vectors,
        lemma_idf=lemma_idf,
    )


def create_search_engine() -> SearchEngine:
    cached_engine = load_search_engine_from_tfidf(DEFAULT_LEMMA_TFIDF_DIR, DEFAULT_INDEX_FILE)
    if cached_engine is not None:
        return cached_engine

    (
        _tokens,
        _lemma_to_tokens,
        _inverted_index,
        document_urls,
        document_stats,
        _term_idf,
        lemma_idf,
    ) = collect_corpus_data(DEFAULT_OUTPUT_DIR)

    return SearchEngine(
        document_urls=document_urls,
        document_vectors=build_lemma_document_vectors(document_stats, lemma_idf),
        lemma_idf=lemma_idf,
    )


app = Flask(__name__)
search_engine = create_search_engine()


@app.get("/")
def index():
    query = request.args.get("q", "").strip()
    top_k_raw = request.args.get("top_k", "10").strip()

    try:
        top_k = max(1, min(int(top_k_raw), 50))
    except ValueError:
        top_k = 10

    results: list[dict[str, str]] = []
    if query:
        matches = run_vector_search(
            query=query,
            document_vectors=search_engine.document_vectors,
            lemma_idf=search_engine.lemma_idf,
            document_urls=search_engine.document_urls,
            top_k=top_k,
        )
        results = [
            {"document": document, "url": url, "score": f"{score:.6f}"}
            for document, url, score in matches
        ]

    return render_template(
        "index.html",
        query=query,
        top_k=top_k,
        results=results,
    )


if __name__ == "__main__":
    app.run(debug=False)
