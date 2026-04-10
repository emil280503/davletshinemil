# PythonProject2

Скрипт и веб-приложение для скачивания русскоязычных HTML-страниц, извлечения токенов, лемматизации, построения индекса, булевого поиска, расчета TF-IDF и векторного поиска.

## Что делает

- скачивает не менее 100 HTML-страниц из `urls.txt`;
- сохраняет страницы в `downloaded_pages/`;
- строит `index.txt` со связкой `файл -> URL`;
- извлекает видимый текст из HTML без `script/style/noscript`;
- формирует `tokens.txt` и `lemmas.txt` как общие служебные списки по всему корпусу;
- формирует `document_tokens/NNN.txt` с уникальными токенами конкретного документа;
- формирует `document_lemmas/NNN.txt` в формате `лемма токен1 токен2 ...` для конкретного документа;
- формирует `inverted_index.txt` в формате `термин документ1 документ2 ...`;
- выполняет булев поиск по запросам с `AND`, `OR`, `NOT` и круглыми скобками;
- считает `tf-idf` по терминам для каждого документа в каталоге `term_tfidf/`;
- считает `tf-idf` по леммам для каждого документа в каталоге `lemma_tfidf/`;
- выполняет векторный поиск по лемматизированному TF-IDF-представлению документов;
- поднимает веб-интерфейс для поиска через браузер.

## Зависимости

```bash
python -m pip install -r requirements.txt
```

## Запуск CLI

Полный цикл:

```bash
python main.py --urls urls.txt --output downloaded_pages --index index.txt --tokens tokens.txt --lemmas lemmas.txt --inverted-index inverted_index.txt --term-tfidf-dir term_tfidf --lemma-tfidf-dir lemma_tfidf
```

Только обработка уже сохраненных документов:

```bash
python main.py --skip-download
```

Булев поиск:

```bash
python main.py --skip-download --query "(Клеопатра AND Цезарь) OR (Антоний AND Цицерон) OR Помпей"
```

Векторный поиск:

```bash
python main.py --skip-download --vector-query "москва столица река" --top-k 10
```

## Запуск сайта

```bash
python app.py
```

После запуска открой `http://127.0.0.1:5000/`.

Если сайт открывается не сразу, сначала один раз пересчитай артефакты:

```bash
python main.py --skip-download
```

## Результат

- `tokens.txt` - по одному токену на строку;
- `lemmas.txt` - по одной лемме на строку, затем все относящиеся к ней токены;
- `document_tokens/NNN.txt` - токены одного документа, по одному токену на строку;
- `document_lemmas/NNN.txt` - леммы одного документа в формате `лемма токен1 токен2 ...`;
- `inverted_index.txt` - по одному термину на строку, затем список документов;
- `term_tfidf/NNN.txt` - для каждого документа строки в формате `термин idf tf-idf`;
- `lemma_tfidf/NNN.txt` - для каждого документа строки в формате `лемма idf tf-idf`;
- `index.txt` - соответствие сохраненных страниц исходным URL.
