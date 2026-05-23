# Third-Party Notices

This project uses third-party components. When distributing this software (including
as a compiled executable), you must include the applicable license texts and copyright
notices. Full license texts for all bundled dependencies are generated at build time
as `THIRD_PARTY_LICENSES.txt` and are included in the GitHub Release.

## Direct dependencies (pyproject.toml)

- FastAPI — MIT — https://github.com/fastapi/fastapi
- Uvicorn — BSD 3-Clause — https://github.com/encode/uvicorn
- Jinja2 — BSD 3-Clause — https://github.com/pallets/jinja
- python-multipart — Apache-2.0 — https://github.com/Kludex/python-multipart
- jsonschema — MIT — https://github.com/python-jsonschema/jsonschema
- SQLAlchemy — MIT — https://github.com/sqlalchemy/sqlalchemy
- aiosqlite — MIT — https://github.com/omnilib/aiosqlite
- TinyDB — MIT — https://github.com/msiemens/tinydb
- filelock — The Unlicense — https://github.com/tox-dev/filelock
- orjson — Apache-2.0 / MIT — https://github.com/ijl/orjson
- ulid-py — Apache-2.0 — https://github.com/ahawker/ulid
- Typer — MIT — https://github.com/fastapi/typer
- httpx — BSD 3-Clause — https://github.com/encode/httpx
- user-permission — MIT — https://github.com/mokuichi147/user-permission
- openpyxl — MIT — https://foss.heptapod.net/openpyxl/openpyxl
- pyarrow — Apache-2.0 — https://github.com/apache/arrow

## Key transitive dependencies also bundled in the executable

- Starlette — BSD 3-Clause — https://github.com/encode/starlette
- Pydantic — MIT — https://github.com/pydantic/pydantic
- pydantic-core — MIT — https://github.com/pydantic/pydantic-core
- Click — BSD 3-Clause — https://github.com/pallets/click
- Rich — MIT — https://github.com/Textualize/rich
- AnyIO — MIT — https://github.com/agronholm/anyio
- httpcore — BSD 3-Clause — https://github.com/encode/httpcore
- h11 — MIT — https://github.com/python-hyper/h11
- certifi — MPL-2.0 — https://github.com/certifi/python-certifi
- idna — BSD-like — https://github.com/kjd/idna
- sniffio — MIT / Apache-2.0 — https://github.com/python-trio/sniffio
- greenlet — MIT — https://github.com/python-greenlet/greenlet
- MarkupSafe — BSD 3-Clause — https://github.com/pallets/markupsafe
- attrs — MIT — https://github.com/python-attrs/attrs
- jsonschema-specifications — MIT — https://github.com/python-jsonschema/jsonschema-specifications
- referencing — MIT — https://github.com/python-jsonschema/referencing
- rpds-py — MIT — https://github.com/crate-py/rpds
- annotated-types — MIT — https://github.com/annotated-types/annotated-types
- typing_extensions — PSF-2.0 — https://github.com/python/typing_extensions
- typing-inspection — MIT — https://github.com/pydantic/typing-inspection
- shellingham — MIT — https://github.com/sarugaku/shellingham
- colorama — BSD 3-Clause — https://github.com/tartley/colorama
- Pygments — BSD 2-Clause — https://github.com/pygments/pygments
- markdown-it-py — MIT — https://github.com/executablebooks/markdown-it-py
- mdurl — MIT — https://github.com/executablebooks/mdurl
- et-xmlfile — MIT — https://foss.heptapod.net/openpyxl/et_xmlfile
- PyInstaller bootloader — Apache-2.0 (with distribution exception) — https://github.com/pyinstaller/pyinstaller

## Frontend CDN libraries

- Tailwind CSS — MIT — https://github.com/tailwindlabs/tailwindcss
- htmx — BSD 2-Clause — https://htmx.org
- SortableJS — MIT — https://github.com/SortableJS/Sortable
- flatpickr — MIT — https://github.com/flatpickr/flatpickr
- Chart.js — MIT — https://github.com/chartjs/Chart.js

## Notes on specific licenses

**MPL-2.0 (certifi):** The Mozilla Public License 2.0 allows distribution of certifi as
part of a larger work. The source code for certifi is available at
https://github.com/certifi/python-certifi. No modifications were made to certifi.

**PyInstaller bootloader:** PyInstaller ≥ 4.2 ships a bootloader under Apache-2.0 with
an explicit exception permitting distribution in executables of any license. The bundled
executable is therefore not subject to GPL restrictions.
