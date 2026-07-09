#!/usr/bin/env python3
"""
Полный анализатор N.I.N.A. Advanced API OpenAPI спецификации.

Возможности:
- Парсит ВСЕ эндпоинты из OpenAPI 3.0 spec
- Показывает параметры (query/path/body)
- Группирует по тегам
- Ищет по ключевым словам (autofocus, trigger, sequence, etc.)
- Генерирует готовый маппинг для trigger_emulator.py
- Экспортирует в Markdown и JSON

Использование:
    python scripts/analyze_nina_api.py
    python scripts/analyze_nina_api.py --search autofocus
    python scripts/analyze_nina_api.py --tag Focuser
    python scripts/analyze_nina_api.py --export-markdown api_reference.md
"""

import sys
import json
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field


import logging

logger = logging.getLogger("AnalyzeNinaAPI")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


@dataclass
class APIParameter:
    """Параметр API эндпоинта."""

    name: str
    location: str  # query, path, header, body
    param_type: str
    required: bool = False
    description: str = ""
    example: Optional[Any] = None
    enum: Optional[List[str]] = None
    default: Optional[Any] = None


@dataclass
class APIEndpoint:
    """Полное описание API эндпоинта."""

    path: str
    method: str
    summary: str = ""
    description: str = ""
    tags: List[str] = field(default_factory=list)
    parameters: List[APIParameter] = field(default_factory=list)
    has_request_body: bool = False
    responses: Dict[str, str] = field(default_factory=dict)

    @property
    def full_signature(self) -> str:
        """Возвращает читаемую сигнатуру эндпоинта."""
        params_str = []
        for p in self.parameters:
            if p.location == "path":
                params_str.append(f"{{{p.name}}}")
            elif p.location == "query":
                req = "*" if p.required else "?"
                params_str.append(f"{p.name}{req}:{p.param_type}")
        params_part = ", ".join(params_str) if params_str else ""
        return f"{self.method:6s} {self.path} ({params_part})"


class OpenAPIAnalyzer:
    """Детальный анализатор OpenAPI 3.0 спецификации."""

    def __init__(self, spec_path: Path):
        if not spec_path.exists():
            raise FileNotFoundError(f"OpenAPI spec not found: {spec_path}")

        with open(spec_path, "r", encoding="utf-8") as f:
            self.spec = json.load(f)

        self.info = self.spec.get("info", {})
        self.servers = self.spec.get("servers", [])
        self.paths = self.spec.get("paths", {})
        self.components = self.spec.get("components", {})

        self.base_url = self.servers[0]["url"] if self.servers else ""
        self.endpoints: List[APIEndpoint] = []

        self._parse_all_endpoints()

    def _parse_all_endpoints(self):
        """Парсит все эндпоинты из paths."""
        for path, methods in self.paths.items():
            for method, details in methods.items():
                if method.lower() not in ("get", "post", "put", "delete", "patch"):
                    continue

                endpoint = APIEndpoint(
                    path=path,
                    method=method.upper(),
                    summary=details.get("summary", ""),
                    description=details.get("description", ""),
                    tags=details.get("tags", []),
                    has_request_body="requestBody" in details,
                )

                # Парсим параметры
                for param in details.get("parameters", []):
                    schema = param.get("schema", {})
                    endpoint.parameters.append(
                        APIParameter(
                            name=param.get("name", ""),
                            location=param.get("in", "query"),
                            param_type=schema.get("type", "string"),
                            required=param.get("required", False),
                            description=param.get("description", ""),
                            example=schema.get("example"),
                            enum=schema.get("enum"),
                            default=schema.get("default"),
                        )
                    )

                # Парсим responses
                for status, resp in details.get("responses", {}).items():
                    endpoint.responses[status] = resp.get("description", "")

                self.endpoints.append(endpoint)

    def get_all_tags(self) -> List[str]:
        """Возвращает все уникальные теги."""
        tags = set()
        for ep in self.endpoints:
            tags.update(ep.tags)
        return sorted(tags)

    def get_endpoints_by_tag(self, tag: str) -> List[APIEndpoint]:
        """Возвращает эндпоинты по тегу."""
        return [ep for ep in self.endpoints if tag in ep.tags]

    def search(self, query: str) -> List[APIEndpoint]:
        """Ищет эндпоинты по ключевому слову."""
        query_lower = query.lower()
        results = []

        for ep in self.endpoints:
            searchable = " ".join(
                [
                    ep.path,
                    ep.summary,
                    ep.description,
                    " ".join(ep.tags),
                    " ".join(p.name for p in ep.parameters),
                ]
            ).lower()

            if query_lower in searchable:
                results.append(ep)

        return results

    def get_stats(self) -> Dict[str, Any]:
        """Статистика по спецификации."""
        methods_count = {}
        for ep in self.endpoints:
            methods_count[ep.method] = methods_count.get(ep.method, 0) + 1

        return {
            "title": self.info.get("title"),
            "version": self.info.get("version"),
            "base_url": self.base_url,
            "total_endpoints": len(self.endpoints),
            "total_tags": len(self.get_all_tags()),
            "methods": methods_count,
            "tags": self.get_all_tags(),
        }


def print_endpoint_detailed(ep: APIEndpoint, base_url: str = ""):
    """Детальный вывод одного эндпоинта."""
    print(f"\n  {'─' * 76}")
    print(f"  {ep.method:6s} {base_url}{ep.path}")
    if ep.summary:
        print(f"  📝 {ep.summary}")
    if ep.description:
        # Обрезаем длинное описание
        desc = (
            ep.description[:200] + "..."
            if len(ep.description) > 200
            else ep.description
        )
        print(f"     {desc}")

    # Параметры
    if ep.parameters:
        path_params = [p for p in ep.parameters if p.location == "path"]
        query_params = [p for p in ep.parameters if p.location == "query"]

        if path_params:
            print(f"  🔗 Path params:")
            for p in path_params:
                print(f"     • {{{p.name}}} ({p.param_type})")

        if query_params:
            print(f"  🔧 Query params:")
            for p in query_params:
                req = " (required)" if p.required else ""
                enum_str = f" [{', '.join(map(str, p.enum))}]" if p.enum else ""
                example_str = f" = {p.example}" if p.example is not None else ""
                print(f"     • {p.name}: {p.param_type}{req}{enum_str}{example_str}")
                if p.description:
                    print(f"       {p.description[:100]}")

    if ep.has_request_body:
        print(f"  📦 Has request body (POST/PUT)")


def generate_trigger_mapping(analyzer: OpenAPIAnalyzer) -> Dict[str, Dict]:
    """Генерирует маппинг триггеров для trigger_emulator.py."""
    mapping = {}

    # Ключевые слова для поиска триггеров
    trigger_patterns = {
        "autofocus": ["auto-focus", "autofocus", "auto_focus"],
        "dither": ["dither"],
        "guider_start": ["guider/start", "startguiding"],
        "guider_stop": ["guider/stop", "stopguiding"],
        "guider_calibrate": ["guider/start?calibrate", "calibrate"],
        "sequence_start": ["sequence/start", "startsequence"],
        "sequence_stop": ["sequence/stop", "stopsequence"],
        "mount_park": ["mount/park"],
        "mount_unpark": ["mount/unpark"],
        "mount_home": ["mount/home"],
        "meridian_flip": ["mount/flip", "meridian"],
        "dome_park": ["dome/park"],
        "dome_open": ["dome/open"],
        "dome_close": ["dome/close"],
        "camera_connect": ["camera/connect"],
        "camera_cool": ["camera/cool"],
        "camera_warm": ["camera/warm"],
        "flat_wizard": ["flats/skyflat", "flats/auto"],
        "livestack_start": ["livestack/start"],
        "livestack_stop": ["livestack/stop"],
    }

    for trigger_name, patterns in trigger_patterns.items():
        matches = []
        for pattern in patterns:
            matches.extend(analyzer.search(pattern))

        # Убираем дубликаты
        seen_paths = set()
        unique_matches = []
        for m in matches:
            if m.path not in seen_paths:
                seen_paths.add(m.path)
                unique_matches.append(m)

        if unique_matches:
            mapping[trigger_name] = {
                "primary": {
                    "method": unique_matches[0].method,
                    "path": unique_matches[0].path,
                    "summary": unique_matches[0].summary,
                    "parameters": [
                        {
                            "name": p.name,
                            "location": p.location,
                            "type": p.param_type,
                            "required": p.required,
                        }
                        for p in unique_matches[0].parameters
                    ],
                },
                "alternatives": [
                    {"method": m.method, "path": m.path} for m in unique_matches[1:3]
                ],
            }

    return mapping


def main():
    parser = argparse.ArgumentParser(
        description="Детальный анализатор N.I.N.A. Advanced API OpenAPI спецификации"
    )
    parser.add_argument(
        "--spec",
        type=Path,
        default=Path("../config/nina_api_spec.json"),
        help="Путь к OpenAPI spec (JSON или YAML)",
    )
    parser.add_argument("--search", "-s", type=str, help="Поиск по ключевому слову")
    parser.add_argument("--tag", "-t", type=str, help="Фильтр по тегу")
    parser.add_argument(
        "--triggers", action="store_true", help="Показать маппинг триггеров"
    )
    parser.add_argument(
        "--stats", action="store_true", help="Показать только статистику"
    )
    parser.add_argument("--export-markdown", type=Path, help="Экспорт в Markdown файл")
    parser.add_argument(
        "--export-json", type=Path, help="Экспорт маппинга триггеров в JSON"
    )
    parser.add_argument(
        "--detailed",
        "-d",
        action="store_true",
        help="Подробный вывод каждого эндпоинта",
    )

    args = parser.parse_args()

    # Загружаем спецификацию
    try:
        analyzer = OpenAPIAnalyzer(args.spec)
    except FileNotFoundError as e:
        print(f"❌ {e}")
        print("\nСкачайте спецификацию:")
        print(
            "  Invoke-WebRequest -Uri 'https://christian-photo.github.io/github-page/projects/ninaAPI/v2/doc/api.json' -OutFile '../config/nina_api_spec.json'"
        )
        sys.exit(1)

    stats = analyzer.get_stats()

    # === Статистика ===
    print("=" * 80)
    print(f"📖 {stats['title']} v{stats['version']}")
    print(f"🌐 Base URL: {stats['base_url']}")
    print(f"📊 Total endpoints: {stats['total_endpoints']}")
    print(f"🏷️  Tags ({stats['total_tags']}): {', '.join(stats['tags'])}")
    print(f"🔧 Methods: {stats['methods']}")
    print("=" * 80)

    if args.stats:
        return

    # === Поиск ===
    if args.search:
        print(f"\n🔍 Поиск по: '{args.search}'")
        results = analyzer.search(args.search)
        print(f"   Найдено эндпоинтов: {len(results)}")

        for ep in results:
            if args.detailed:
                print_endpoint_detailed(ep, analyzer.base_url)
            else:
                print(f"  • {ep.full_signature}")
                if ep.summary:
                    print(f"    {ep.summary}")
        return

    # === Фильтр по тегу ===
    if args.tag:
        print(f"\n🏷️  Тег: '{args.tag}'")
        results = analyzer.get_endpoints_by_tag(args.tag)
        print(f"   Эндпоинтов: {len(results)}")

        for ep in results:
            if args.detailed:
                print_endpoint_detailed(ep, analyzer.base_url)
            else:
                print(f"  • {ep.full_signature}")
                if ep.summary:
                    print(f"    {ep.summary}")
        return

    # === Маппинг триггеров ===
    if args.triggers or args.export_json:
        mapping = generate_trigger_mapping(analyzer)

        if args.triggers:
            print("\n" + "=" * 80)
            print("🎯 TRIGGER MAPPING (для trigger_emulator.py)")
            print("=" * 80)

            for trigger_name, data in mapping.items():
                primary = data["primary"]
                print(f"\n  🔥 {trigger_name}")
                print(
                    f"     {primary['method']:6s} {analyzer.base_url}{primary['path']}"
                )
                print(f"     📝 {primary['summary']}")

                if primary["parameters"]:
                    print(f"     🔧 Parameters:")
                    for p in primary["parameters"]:
                        req = " (required)" if p["required"] else ""
                        print(
                            f"        • {p['name']}: {p['type']} ({p['location']}){req}"
                        )

                if data["alternatives"]:
                    print(f"     🔄 Alternatives:")
                    for alt in data["alternatives"]:
                        print(f"        • {alt['method']:6s} {alt['path']}")

        if args.export_json:
            export_path = Path(args.export_json)
            export_path.parent.mkdir(parents=True, exist_ok=True)  # ← добавить

            with open(export_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "base_url": analyzer.base_url,
                        "triggers": mapping,
                    },
                    f,
                    indent=2,
                    ensure_ascii=False,
                )
            print(f"\n💾 Trigger mapping saved to: {export_path}")

        if not args.triggers:
            return

    # === Полный список по тегам ===
    if not (args.search or args.tag or args.triggers):
        print("\n" + "=" * 80)
        print("📋 ENDPOINTS BY TAG")
        print("=" * 80)

        for tag in analyzer.get_all_tags():
            endpoints = analyzer.get_endpoints_by_tag(tag)
            print(f"\n🏷️  {tag} ({len(endpoints)} endpoints)")
            print("─" * 80)

            for ep in endpoints:
                if args.detailed:
                    print_endpoint_detailed(ep, analyzer.base_url)
                else:
                    print(f"  {ep.full_signature}")
                    if ep.summary:
                        print(f"    └─ {ep.summary}")

    # === Экспорт в Markdown ===
    if args.export_markdown:
        export_to_markdown(analyzer, args.export_markdown)


def export_to_markdown(analyzer: OpenAPIAnalyzer, output_path: Path):
    """Экспорт полной документации в Markdown."""
    # ИСПРАВЛЕНО: Создаём родительскую директорию если её нет
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"# {analyzer.info.get('title', 'API Documentation')}\n\n")
        f.write(f"**Version:** {analyzer.info.get('version')}\n\n")
        f.write(f"**Base URL:** `{analyzer.base_url}`\n\n")
        f.write(f"{analyzer.info.get('description', '')}\n\n")

        for tag in analyzer.get_all_tags():
            endpoints = analyzer.get_endpoints_by_tag(tag)
            f.write(f"\n## {tag}\n\n")

            for ep in endpoints:
                f.write(f"### `{ep.method} {ep.path}`\n\n")
                f.write(f"**{ep.summary}**\n\n")
                if ep.description:
                    f.write(f"{ep.description}\n\n")

                if ep.parameters:
                    f.write("#### Parameters\n\n")
                    f.write("| Name | Location | Type | Required | Description |\n")
                    f.write("|------|----------|------|----------|-------------|\n")
                    for p in ep.parameters:
                        req = "✓" if p.required else ""
                        f.write(
                            f"| `{p.name}` | {p.location} | {p.param_type} | {req} | {p.description} |\n"
                        )
                    f.write("\n")

    print(f"\n📄 Documentation exported to: {output_path}")


if __name__ == "__main__":
    main()
