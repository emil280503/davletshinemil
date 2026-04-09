# PythonProject2

Скрипт для скачивания русскоязычных HTML-страниц, извлечения из них чистых токенов и группировки токенов по леммам.

## Что делает

- скачивает не менее 100 HTML-страниц из `urls.txt`;
- сохраняет страницы в `downloaded_pages/`;
- строит `index.txt` со связкой `файл -> URL`;
- извлекает видимый текст из HTML без `script/style/noscript`;
- формирует `tokens.txt` без дублей, чисел, служебных слов и мусорных фрагментов;
- формирует `lemmas.txt` в формате `лемма токен1 токен2 ...`.

## Зависимости

```bash
python -m pip install -r requirements.txt
```

## Запуск

Полный цикл:

```bash
python main.py --urls urls.txt --output downloaded_pages --index index.txt --tokens tokens.txt --lemmas lemmas.txt
```

Только обработка уже сохраненных документов:

```bash
python main.py --skip-download --output downloaded_pages --tokens tokens.txt --lemmas lemmas.txt
```

## Результат

- `tokens.txt` - по одному токену на строку;
- `lemmas.txt` - по одной лемме на строку, затем все относящиеся к ней токены;
- `index.txt` - соответствие сохраненных страниц исходным URL.
