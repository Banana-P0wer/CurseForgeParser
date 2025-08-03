#!/usr/bin/env python3
# -*- coding: utf-8 -*-

DEFAULT_CSV_PATH = "curseforge_dataset.csv"
DEFAULT_LOG_PATH = "curseforge.log"

CSV_HEADERS = [
    "id",                 # числовой идентификатор проекта на CurseForge
    "slug",               # человекочитаемый идентификатор (для URL)
    "name",               # название проекта
    "description",        # краткое описание проекта
    "created_at",         # дата создания проекта
    "updated_at",         # дата последнего обновления
    "downloads",          # общее число загрузок, integer
    "size",               # размер последнего релиза, строка "1.38 MB"
    "game_version",       # основная/последняя поддерживаемая версия
    "is_forge",           # True/False
    "is_fabric",          # True/False
    "is_neoforge",        # True/False
    "is_quilt",           # True/False
    "authors",            # "A; B; C"
    "categories",         # "Utility; Performance"
    "license",            # тип лицензии, если указан
    "project_url",        # ссылка на страницу проекта
    "crawled_at"          # время сбора данных
]


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12
}


def main():
    pass


if __name__ == "__main__":
    main()
