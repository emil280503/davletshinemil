# PythonProject2

Скрипт для скачивания русскоязычных HTML-страниц, извлечения токенов, лемматизации, построения инвертированного индекса и булевого поиска.

## Что делает

- скачивает не менее 100 HTML-страниц из `urls.txt`;
- сохраняет страницы в `downloaded_pages/`;
- строит `index.txt` со связкой `файл -> URL`;
- извлекает видимый текст из HTML без `script/style/noscript`;
- формирует `tokens.txt` без дублей, чисел, служебных слов и мусора;
- формирует `lemmas.txt` в формате `лемма токен1 токен2 ...`;
- формирует `inverted_index.txt` в формате `термин документ1 документ2 ...`;
- выполняет булев поиск по запросам с `AND`, `OR`, `NOT` и круглыми скобками.

## Зависимости

```bash
python -m pip install -r requirements.txt
```

## Запуск

Полный цикл:

```bash
python main.py --urls urls.txt --output downloaded_pages --index index.txt --tokens tokens.txt --lemmas lemmas.txt --inverted-index inverted_index.txt
```

Только обработка уже сохраненных документов:

```bash
python main.py --skip-download --output downloaded_pages --tokens tokens.txt --lemmas lemmas.txt --inverted-index inverted_index.txt
```

Булев поиск по строке запроса:

```bash
python main.py --skip-download --query "(Клеопатра AND Цезарь) OR (Антоний AND Цицерон) OR Помпей"
```

## Результат

- `tokens.txt` - по одному токену на строку;
- `lemmas.txt` - по одной лемме на строку, затем все относящиеся к ней токены;
- `inverted_index.txt` - по одному термину на строку, затем список документов;
- `index.txt` - соответствие сохраненных страниц исходным URL.
