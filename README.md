# PythonProject2

Скрипт для скачивания русскоязычных HTML-страниц, извлечения токенов, лемматизации, построения индекса, булевого поиска и расчета TF-IDF.

## Что делает

- скачивает не менее 100 HTML-страниц из `urls.txt`;
- сохраняет страницы в `downloaded_pages/`;
- строит `index.txt` со связкой `файл -> URL`;
- извлекает видимый текст из HTML без `script/style/noscript`;
- формирует `tokens.txt` без дублей, чисел, служебных слов и мусора;
- формирует `lemmas.txt` в формате `лемма токен1 токен2 ...`;
- формирует `inverted_index.txt` в формате `термин документ1 документ2 ...`;
- выполняет булев поиск по запросам с `AND`, `OR`, `NOT` и круглыми скобками;
- считает `tf-idf` по терминам для каждого документа в каталоге `term_tfidf/`;
- считает `tf-idf` по леммам для каждого документа в каталоге `lemma_tfidf/`.

## Зависимости

```bash
python -m pip install -r requirements.txt
```

## Запуск

Полный цикл:

```bash
python main.py --urls urls.txt --output downloaded_pages --index index.txt --tokens tokens.txt --lemmas lemmas.txt --inverted-index inverted_index.txt --term-tfidf-dir term_tfidf --lemma-tfidf-dir lemma_tfidf
```

Только обработка уже сохраненных документов:

```bash
python main.py --skip-download
```

Булев поиск по строке запроса:

```bash
python main.py --skip-download --query "(Клеопатра AND Цезарь) OR (Антоний AND Цицерон) OR Помпей"
```

## Результат

- `tokens.txt` - по одному токену на строку;
- `lemmas.txt` - по одной лемме на строку, затем все относящиеся к ней токены;
- `inverted_index.txt` - по одному термину на строку, затем список документов;
- `term_tfidf/NNN.txt` - для каждого документа строки в формате `термин idf tf-idf`;
- `lemma_tfidf/NNN.txt` - для каждого документа строки в формате `лемма idf tf-idf`;
- `index.txt` - соответствие сохраненных страниц исходным URL.
