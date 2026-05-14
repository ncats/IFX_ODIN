import html
import json
from pathlib import Path


SOURCE_DIR = Path("/Users/kelleherkj/IdeaProjects/project-cure-backend/server/apps/ui_forms/json_data")
FORM_PATH = SOURCE_DIR / "rasopathies.json"
MENU_PATH = SOURCE_DIR / "rasopathies-menu.json"
OUTPUT_PATH = Path("/Users/kelleherkj/IdeaProjects/IFX_ODIN/designs/rasopathies_form_preview.html")


def esc(value):
    return html.escape("" if value is None else str(value))


def load_json(path: Path):
    return json.loads(path.read_text())


def render_options(options, control_type):
    if not options:
        return ""
    items = []
    for option in options:
        if isinstance(option, dict):
            label = option.get("label") or option.get("value") or option.get("name") or ""
        else:
            label = str(option)
        items.append(f'<span class="option-pill">{esc(label)}</span>')
    wrapper_class = "option-grid option-grid-compact" if control_type in {"radio", "checkbox"} else "option-grid"
    return f'<div class="{wrapper_class}">{"".join(items)}</div>'


def render_grouped_options(groups):
    blocks = []
    for group in groups or []:
        group_label = group.get("label") or "Group"
        default_flag = '<span class="meta-badge">Default bucket</span>' if group.get("default") else ""
        blocks.append(
            f"""
            <div class="group-block">
                <div class="group-title-row">
                    <h5>{esc(group_label)}</h5>
                    {default_flag}
                </div>
                {render_options(group.get("options") or [], "checkbox")}
            </div>
            """
        )
    return "".join(blocks)


def render_control(control):
    control_type = control.get("controlType", "unknown")
    label = control.get("label") or control.get("title") or control.get("key") or control_type
    description = control.get("description")
    note = control.get("note") or control.get("noteAbove")
    placeholder = control.get("placeholder")
    required = control.get("required")
    key = control.get("key")

    meta_parts = [control_type]
    if key:
        meta_parts.append(key)
    if required:
        meta_parts.append("required")
    meta_html = "".join(f'<span class="meta-badge">{esc(part)}</span>' for part in meta_parts)

    body = ""
    if control_type == "label":
        body = f'<div class="label-note">{label}</div>'
    elif control_type in {"textbox", "textarea", "autocomplete"}:
        body = f'<div class="input-mock">{esc(placeholder or "Enter value")}</div>'
    elif control_type == "dropdown":
        body = f'<div class="input-mock">{esc(placeholder or "Select")}</div>{render_options(control.get("options") or [], control_type)}'
    elif control_type in {"radio", "checkbox"}:
        options = control.get("options") or control.get("alternative_options") or []
        body = render_options(options, control_type)
    elif control_type == "multiselect":
        if control.get("groups"):
            body = render_grouped_options(control.get("groups"))
        else:
            body = f'<div class="input-mock">{esc(placeholder or "Select")}</div>{render_options(control.get("options") or [], control_type)}'
    elif control_type == "file":
        body = '<div class="file-mock">Upload area for images / PDFs / documents</div>'
    elif control_type == "dialog":
        inner = []
        autocomplete = control.get("autocompleteInput")
        if autocomplete:
            inner.append(
                f"""
                <div class="dialog-intro">
                    <div class="section-kicker">Entry point</div>
                    {render_control(autocomplete)}
                </div>
                """
            )
        for child in control.get("controls") or []:
            inner.append(render_control(child))
        body = f'<div class="dialog-shell">{"".join(inner)}</div>'
    elif control_type == "group":
        children = "".join(render_control(child) for child in (control.get("controls") or []))
        body = f'<div class="group-shell">{children}</div>'
    elif control_type == "accordion":
        groups = []
        for group in control.get("groups") or []:
            controls = "".join(render_control(child) for child in (group.get("controls") or []))
            groups.append(
                f"""
                <details class="accordion-block" open>
                    <summary>{esc(group.get("label") or group.get("key") or "Section")}</summary>
                    <div class="accordion-body">{controls}</div>
                </details>
                """
            )
        body = "".join(groups)
    elif control_type in {"overview", "json"}:
        entries = control.get("value") or {}
        items = "".join(
            f'<div class="kv-row"><span class="kv-key">{esc(k)}</span><span class="kv-val">{esc(v)}</span></div>'
            for k, v in entries.items()
        )
        body = f'<div class="summary-shell">{items}</div>'
    else:
        body = f'<div class="unknown-control">Unsupported preview control: {esc(control_type)}</div>'

    description_html = f'<p class="control-description">{esc(description)}</p>' if description else ""
    note_html = f'<p class="control-note">{esc(note)}</p>' if note else ""
    title_html = "" if control_type == "label" else f'<h4>{esc(label)}</h4>'
    return f"""
    <section class="control-card">
        <div class="control-meta">{meta_html}</div>
        {title_html}
        {description_html}
        {note_html}
        {body}
    </section>
    """


def render_page(page_name, page_spec):
    controls = "".join(render_control(control) for control in (page_spec.get("formControls") or []))
    description = page_spec.get("description") or ""
    return f"""
    <section class="page-card" id="{esc(page_name)}">
        <div class="page-header">
            <div>
                <div class="section-kicker">{esc(page_name)}</div>
                <h2>{esc(page_spec.get("title") or page_name)}</h2>
            </div>
            <div class="page-flags">
                <span class="meta-badge">showPage={esc(page_spec.get("showPage"))}</span>
                <span class="meta-badge">required={esc(page_spec.get("isRequired"))}</span>
            </div>
        </div>
        {f'<p class="page-description">{esc(description)}</p>' if description else ''}
        <div class="page-controls">{controls}</div>
    </section>
    """


def render_variant(variant_name, menu_spec, form_spec):
    pages = []
    nav_items = []
    for item in menu_spec.get("menu") or []:
        page_name = item.get("name")
        page_spec = form_spec.get(page_name)
        if not page_spec:
            continue
        nav_items.append(f'<a href="#{esc(page_name)}">{esc(item.get("title") or page_name)}</a>')
        pages.append(render_page(page_name, page_spec))
    return f"""
    <section class="variant-shell" data-variant="{esc(variant_name)}">
        <aside class="variant-nav">
            <h3>{esc(variant_name)}</h3>
            {''.join(nav_items)}
        </aside>
        <main class="variant-pages">
            {''.join(pages)}
        </main>
    </section>
    """


def build_html():
    form_spec = load_json(FORM_PATH)
    menu_spec = load_json(MENU_PATH)

    variants = []
    variant_tabs = []
    for idx, (variant_name, variant_menu) in enumerate(menu_spec.items()):
        active = "is-active" if idx == 0 else ""
        variants.append(render_variant(variant_name, variant_menu, form_spec))
        variant_tabs.append(
            f'<button class="variant-tab {active}" data-target="{esc(variant_name)}">{esc(variant_name)}</button>'
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Rasopathies Form Preview</title>
  <style>
    :root {{
      --bg: #f4efe7;
      --card: #fffdf8;
      --ink: #1f2937;
      --muted: #6b7280;
      --line: #d9cdbd;
      --accent: #9b4d12;
      --accent-soft: #f3e3d4;
      --nav: #204e85;
      --nav-soft: #e6eef8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", Georgia, serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(155, 77, 18, 0.12), transparent 30%),
        linear-gradient(180deg, #f8f4ee, var(--bg));
    }}
    .shell {{
      width: min(1500px, calc(100% - 40px));
      margin: 24px auto 40px;
      display: grid;
      gap: 18px;
    }}
    .hero {{
      padding: 22px 24px;
      border: 1px solid var(--line);
      border-radius: 22px;
      background: linear-gradient(145deg, rgba(32, 78, 133, 0.08), rgba(255,255,255,0.92));
      box-shadow: 0 12px 40px rgba(31, 41, 55, 0.08);
    }}
    .hero h1 {{ margin: 0 0 8px; font-size: 2rem; }}
    .hero p {{ margin: 0; color: var(--muted); line-height: 1.5; max-width: 90ch; }}
    .tab-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }}
    .variant-tab {{
      border: 1px solid var(--line);
      border-radius: 999px;
      background: var(--card);
      padding: 10px 14px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      color: var(--ink);
    }}
    .variant-tab.is-active {{
      background: var(--nav);
      color: white;
      border-color: var(--nav);
    }}
    .variant-shell {{
      display: none;
      grid-template-columns: 280px minmax(0, 1fr);
      gap: 18px;
      align-items: start;
    }}
    .variant-shell.is-active {{ display: grid; }}
    .variant-nav {{
      position: sticky;
      top: 18px;
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 20px;
      background: var(--card);
    }}
    .variant-nav h3 {{ margin: 0 0 12px; font-size: 1.1rem; }}
    .variant-nav a {{
      display: block;
      padding: 8px 10px;
      margin-bottom: 6px;
      color: var(--nav);
      text-decoration: none;
      border-radius: 12px;
      background: var(--nav-soft);
      font-size: 0.95rem;
    }}
    .variant-pages {{
      display: grid;
      gap: 18px;
    }}
    .page-card {{
      padding: 20px;
      border: 1px solid var(--line);
      border-radius: 22px;
      background: var(--card);
      box-shadow: 0 10px 32px rgba(31, 41, 55, 0.06);
    }}
    .page-header {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: start;
      margin-bottom: 12px;
    }}
    .page-header h2 {{ margin: 0; font-size: 1.45rem; }}
    .page-description {{ color: var(--muted); margin: 0 0 14px; }}
    .section-kicker {{
      color: var(--accent);
      font-size: 0.8rem;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      margin-bottom: 6px;
    }}
    .page-flags, .control-meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .meta-badge {{
      display: inline-flex;
      align-items: center;
      padding: 4px 8px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #f8f1e8;
      color: var(--muted);
      font-size: 0.78rem;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    }}
    .page-controls {{
      display: grid;
      gap: 14px;
    }}
    .control-card {{
      padding: 16px;
      border: 1px solid #eadfce;
      border-radius: 18px;
      background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(249,245,238,0.96));
    }}
    .control-card h4 {{
      margin: 10px 0 8px;
      font-size: 1.05rem;
    }}
    .control-description, .control-note {{
      margin: 8px 0;
      color: var(--muted);
      line-height: 1.45;
    }}
    .control-note {{
      padding: 10px 12px;
      border-left: 4px solid var(--accent);
      background: var(--accent-soft);
      color: #6a3a13;
    }}
    .input-mock, .file-mock {{
      padding: 12px 14px;
      border: 1px dashed #c7b9a6;
      border-radius: 14px;
      background: #fbf7f1;
      color: var(--muted);
      margin-bottom: 10px;
    }}
    .option-grid {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .option-grid-compact .option-pill {{
      background: #f4f7fb;
      border-color: #cfdbeb;
      color: #21446f;
    }}
    .option-pill {{
      display: inline-flex;
      align-items: center;
      padding: 8px 10px;
      border-radius: 12px;
      border: 1px solid #dfd4c3;
      background: white;
      font-size: 0.92rem;
    }}
    .group-block {{
      padding: 14px;
      border: 1px solid #ddd2c1;
      border-radius: 16px;
      background: #fcfaf6;
      margin-bottom: 12px;
    }}
    .group-title-row {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      margin-bottom: 10px;
    }}
    .group-title-row h5 {{
      margin: 0;
      font-size: 1rem;
    }}
    .label-note {{
      color: #5f4a32;
      line-height: 1.5;
    }}
    .dialog-shell, .group-shell, .summary-shell {{
      display: grid;
      gap: 12px;
    }}
    .dialog-intro {{
      padding: 12px;
      border-radius: 14px;
      background: #f7efe3;
      border: 1px solid #e6d2b8;
    }}
    .accordion-block {{
      border: 1px solid #ddd2c1;
      border-radius: 14px;
      background: #fff;
      overflow: hidden;
    }}
    .accordion-block summary {{
      padding: 12px 14px;
      cursor: default;
      font-weight: 700;
      background: #faf6f0;
    }}
    .accordion-body {{
      padding: 14px;
      display: grid;
      gap: 12px;
    }}
    .kv-row {{
      display: grid;
      grid-template-columns: minmax(180px, 260px) 1fr;
      gap: 12px;
      padding: 8px 0;
      border-bottom: 1px solid #f0e8dc;
    }}
    .kv-key {{
      font-weight: 700;
      color: #6a3a13;
    }}
    .unknown-control {{
      color: #b42318;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    }}
    @media (max-width: 1080px) {{
      .variant-shell {{
        grid-template-columns: 1fr;
      }}
      .variant-nav {{
        position: static;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>Rasopathies Form Preview</h1>
      <p>This is a standalone preview generated from the backend form JSON in <code>{esc(str(FORM_PATH))}</code> and <code>{esc(str(MENU_PATH))}</code>. It is not the live CURE frontend, but it preserves the actual page flow, prompts, grouped findings buckets, and nested medication sections closely enough to inspect the user-facing conceptual model.</p>
    </section>
    <div class="tab-row">
      {''.join(variant_tabs)}
    </div>
    {''.join(variants)}
  </div>
  <script>
    const tabs = Array.from(document.querySelectorAll('.variant-tab'));
    const variants = Array.from(document.querySelectorAll('.variant-shell'));
    function activateVariant(name) {{
      tabs.forEach(tab => tab.classList.toggle('is-active', tab.dataset.target === name));
      variants.forEach(section => section.classList.toggle('is-active', section.dataset.variant === name));
    }}
    tabs.forEach(tab => tab.addEventListener('click', () => activateVariant(tab.dataset.target)));
    if (tabs.length) activateVariant(tabs[0].dataset.target);
  </script>
</body>
</html>
"""


def main():
    OUTPUT_PATH.write_text(build_html())
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
