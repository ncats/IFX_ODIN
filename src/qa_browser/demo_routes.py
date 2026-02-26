"""
Demo routes for the POUNCE omicsdb_dev2 database.
Wired into app.py via app.include_router(demo_router).

Credentials are pushed in at startup by app.py calling set_mysql_credentials(),
which avoids the __main__ vs module-import double-instance problem.
"""
import asyncio
import time as _time
from urllib.parse import quote as url_quote

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, text, bindparam

DB_NAME = "omicsdb_dev2"

router = APIRouter(prefix="/demo")

_mysql_credentials: dict = {}
_mysql_engines: dict = {}
_templates = None
_filter_cache: dict = {}   # key -> (timestamp, value)
_FILTER_TTL = 300          # seconds — facet counts don't change during a demo session


def _cached(key: str, factory):
    """Return cached value for key, or call factory() and cache the result."""
    now = _time.time()
    if key in _filter_cache:
        ts, val = _filter_cache[key]
        if now - ts < _FILTER_TTL:
            return val
    val = factory()
    _filter_cache[key] = (now, val)
    return val


def set_mysql_credentials(creds: dict):
    global _mysql_credentials
    _mysql_credentials = creds
    _mysql_engines.clear()  # flush any stale engine built before creds arrived


def set_templates(t):
    global _templates
    _templates = t


def _get_engine():
    if DB_NAME not in _mysql_engines:
        host = _mysql_credentials.get("url", "localhost")
        port = _mysql_credentials.get("port", 3306)
        user = _mysql_credentials.get("user", "root")
        password = url_quote(_mysql_credentials.get("password", ""), safe="")
        _mysql_engines[DB_NAME] = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{DB_NAME}",
            pool_pre_ping=True,
            pool_size=10,
            connect_args={"read_timeout": 300, "write_timeout": 300},
        )
    return _mysql_engines[DB_NAME]


def _fetch_column_metadata(conn, rb_ids: list) -> tuple:
    """Fetch biosample chain metadata for a list of run_biosample IDs.

    Returns (condition_groups, col_order, meta_panel_rows).
    condition_groups: [{"label": str, "cols": [{"rb_id": str, "label": str}]}]
    col_order: flat [rb_id, ...] preserving group order
    meta_panel_rows: [{"label": str, "vals": {rb_id: str}}] — all-blank rows omitted
    Falls back gracefully if any join table is missing.
    """
    meta_rows = []
    if rb_ids:
        try:
            meta_rows = conn.execute(text("""
                SELECT
                    rb.id                          AS rb_id,
                    bs.id                          AS biosample_id,
                    rb.biological_replicate_number AS bio_rep,
                    rb.technical_replicate_number  AS tech_rep,
                    rb.run_order,
                    bs.type                        AS biosample_type,
                    bsp.type                       AS biospecimen_type,
                    bsp.organism,
                    e.id                           AS exposure_id,
                    e.concentration,
                    e.concentration_unit,
                    e.duration,
                    e.duration_unit,
                    e.growth_media,
                    GROUP_CONCAT(DISTINCT en.value ORDER BY en.value SEPARATOR ', ')
                                                   AS exposure_names,
                    GROUP_CONCAT(DISTINCT CONCAT(ec.name, ': ', ec.value)
                                 ORDER BY ec.name SEPARATOR ' | ')
                                                   AS exposure_condition
                FROM run_biosample rb
                JOIN biosample_to_run_biosample b2rb ON b2rb.to_id = rb.id
                JOIN biosample bs ON bs.id = b2rb.from_id
                LEFT JOIN biosample_to_biospecimen b2bsp ON b2bsp.from_id = bs.id
                LEFT JOIN biospecimen bsp ON bsp.id = b2bsp.to_id
                LEFT JOIN biosample_to_exposure b2e ON b2e.from_id = bs.id
                LEFT JOIN exposure e ON e.id = b2e.to_id
                LEFT JOIN exposure__names en ON en.parent_id = e.id
                LEFT JOIN exposure__condition ec ON ec.parent_id = e.id
                WHERE rb.id IN :rb_ids
                GROUP BY rb.id, bs.id, e.id
                ORDER BY rb.id, e.id
            """).bindparams(bindparam("rb_ids", expanding=True)),
            {"rb_ids": rb_ids}).mappings().all()
        except Exception as exc:
            import traceback
            print(f"[column-metadata] query failed: {exc}")
            traceback.print_exc()
            meta_rows = []

    rb_meta: dict = {}
    for row in meta_rows:
        rb_id = row["rb_id"]
        if rb_id not in rb_meta:
            rb_meta[rb_id] = {
                "biosample_id":     row["biosample_id"],
                "bio_rep":          row["bio_rep"],
                "tech_rep":         row["tech_rep"],
                "run_order":        row["run_order"],
                "biosample_type":   row["biosample_type"] or "",
                "biospecimen_type": row["biospecimen_type"] or "",
                "organism":         row["organism"] or "",
                "exposures":        [],
            }
        if row["exposure_id"]:
            conc = f"{row['concentration']} {row['concentration_unit']}" \
                   if row["concentration"] else ""
            dur  = f"{row['duration']} {row['duration_unit']}" \
                   if row["duration"] else ""
            rb_meta[rb_id]["exposures"].append({
                "names":     row["exposure_names"] or "",
                "conc":      conc,
                "duration":  dur,
                "media":     row["growth_media"] or "",
                "condition": row["exposure_condition"] or "",
            })

    def _condition_label(meta: dict) -> str:
        if not meta["exposures"]:
            return meta["biosample_type"] or "No exposure"
        parts = []
        for exp in meta["exposures"]:
            label = exp["names"] or "Unknown"
            if exp["conc"]:     label += f" {exp['conc']}"
            if exp["duration"]: label += f" {exp['duration']}"
            parts.append(label)
        return " + ".join(parts)

    bs_to_rbs: dict = {}
    for rb_id in rb_ids:
        meta = rb_meta.get(rb_id)
        bs_id = meta["biosample_id"] if meta else rb_id
        bs_to_rbs.setdefault(bs_id, []).append(rb_id)

    condition_groups = []
    for bs_id, group_rb_ids in bs_to_rbs.items():
        first_meta = rb_meta.get(group_rb_ids[0])
        label = _condition_label(first_meta) if first_meta else (
            group_rb_ids[0].split("-", 1)[1] if "-" in group_rb_ids[0] else group_rb_ids[0]
        )
        cols = []
        for i, rb_id in enumerate(group_rb_ids, 1):
            meta = rb_meta.get(rb_id, {})
            bio_rep = meta.get("bio_rep")
            col_label = f"Rep {bio_rep}" if bio_rep is not None else f"Rep {i}"
            cols.append({"rb_id": rb_id, "label": col_label})
        condition_groups.append({"label": label, "cols": cols})

    col_order = [c["rb_id"] for g in condition_groups for c in g["cols"]]

    META_FIELDS = [
        ("Biosample ID",   lambda m, rb: m.get("biosample_id", rb)),
        ("Exposure",       lambda m, rb: "; ".join(e["names"]    for e in m.get("exposures", []) if e["names"])),
        ("Concentration",  lambda m, rb: "; ".join(e["conc"]     for e in m.get("exposures", []) if e["conc"])),
        ("Duration",       lambda m, rb: "; ".join(e["duration"] for e in m.get("exposures", []) if e["duration"])),
        ("Growth media",   lambda m, rb: "; ".join(e["media"]    for e in m.get("exposures", []) if e["media"])),
        ("Condition",      lambda m, rb: "; ".join(e["condition"] for e in m.get("exposures", []) if e["condition"])),
        ("Tissue / cell",  lambda m, rb: m.get("biospecimen_type", "")),
        ("Organism",       lambda m, rb: m.get("organism", "")),
        ("Bio replicate",  lambda m, rb: str(m["bio_rep"])  if m.get("bio_rep")  is not None else ""),
        ("Tech replicate", lambda m, rb: str(m["tech_rep"]) if m.get("tech_rep") is not None else ""),
    ]

    meta_panel_rows = []
    for field_label, extractor in META_FIELDS:
        vals = {rb_id: extractor(rb_meta.get(rb_id, {}), rb_id) for rb_id in col_order}
        if any(v for v in vals.values()):
            meta_panel_rows.append({"label": field_label, "vals": vals})

    return condition_groups, col_order, meta_panel_rows


@router.get("/genes", response_class=HTMLResponse)
async def demo_genes(
    request: Request,
    page: int = 1,
    page_size: int = 50,
    search: str = "",
    biotype: str = "",
):
    engine = _get_engine()

    where_parts = []
    params: dict = {}
    if search:
        where_parts.append("symbol LIKE :search")
        params["search"] = f"%{search}%"
    if biotype:
        where_parts.append("biotype = :biotype")
        params["biotype"] = biotype

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    with engine.connect() as conn:
        total = conn.execute(
            text(f"SELECT COUNT(*) FROM gene {where_sql}"), params
        ).scalar()

        biotypes = _cached("biotypes", lambda: [
            {"biotype": r["biotype"] or "unknown", "count": r["cnt"]}
            for r in conn.execute(text(
                "SELECT biotype, COUNT(*) as cnt FROM gene "
                "GROUP BY biotype ORDER BY cnt DESC"
            )).mappings().all()
        ])

        offset = (page - 1) * page_size
        rows = conn.execute(text(
            f"SELECT id, symbol, biotype FROM gene {where_sql} "
            f"ORDER BY symbol LIMIT :limit OFFSET :offset"
        ), {**params, "limit": page_size, "offset": offset}).mappings().all()

    total_pages = max(1, (total + page_size - 1) // page_size)

    htmx = request.headers.get("HX-Request") == "true"
    template_name = "demo_genes_rows.html" if htmx else "demo_genes.html"

    ctx = {
        "request": request,
        "genes": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "search": search,
        "biotype": biotype,
        "biotypes": biotypes,
        "db_name": DB_NAME,
    }
    return _templates.TemplateResponse(template_name, ctx)


@router.get("/metabolites", response_class=HTMLResponse)
async def demo_metabolites(
    request: Request,
    page: int = 1,
    page_size: int = 50,
    search: str = "",
):
    engine = _get_engine()

    where_parts = []
    params: dict = {}
    if search:
        where_parts.append("name LIKE :search")
        params["search"] = f"%{search}%"

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    with engine.connect() as conn:
        total = conn.execute(
            text(f"SELECT COUNT(*) FROM metabolite {where_sql}"), params
        ).scalar()

        offset = (page - 1) * page_size
        rows = conn.execute(text(
            f"SELECT id, name FROM metabolite {where_sql} "
            f"ORDER BY name LIMIT :limit OFFSET :offset"
        ), {**params, "limit": page_size, "offset": offset}).mappings().all()

    total_pages = max(1, (total + page_size - 1) // page_size)

    htmx = request.headers.get("HX-Request") == "true"
    template_name = "demo_metabolites_rows.html" if htmx else "demo_metabolites.html"

    ctx = {
        "request": request,
        "metabolites": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "search": search,
        "db_name": DB_NAME,
    }
    return _templates.TemplateResponse(template_name, ctx)


@router.get("/measured-metabolites", response_class=HTMLResponse)
async def demo_measured_metabolites(
    request: Request,
    page: int = 1,
    page_size: int = 50,
    search: str = "",
    id_level: str = "",
):
    engine = _get_engine()

    where_parts = []
    params: dict = {}
    if search:
        where_parts.append("name LIKE :search")
        params["search"] = f"%{search}%"
    if id_level != "":
        if id_level == "null":
            where_parts.append("identification_level IS NULL")
        else:
            where_parts.append("identification_level = :id_level")
            params["id_level"] = int(id_level)

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    with engine.connect() as conn:
        total = conn.execute(
            text(f"SELECT COUNT(*) FROM measured_metabolite {where_sql}"), params
        ).scalar()

        id_levels = _cached("id_levels", lambda: [
            {"level": str(r["lvl"]), "count": r["cnt"]}
            for r in conn.execute(text(
                "SELECT IFNULL(identification_level, 'null') as lvl, COUNT(*) as cnt "
                "FROM measured_metabolite GROUP BY identification_level ORDER BY identification_level"
            )).mappings().all()
            if r["cnt"] > 0
        ])

        offset = (page - 1) * page_size
        rows = conn.execute(text(
            f"SELECT id, name, identification_level FROM measured_metabolite {where_sql} "
            f"ORDER BY name LIMIT :limit OFFSET :offset"
        ), {**params, "limit": page_size, "offset": offset}).mappings().all()

    total_pages = max(1, (total + page_size - 1) // page_size)

    htmx = request.headers.get("HX-Request") == "true"
    template_name = "demo_measured_metabolites_rows.html" if htmx else "demo_measured_metabolites.html"

    ctx = {
        "request": request,
        "metabolites": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "search": search,
        "id_level": id_level,
        "id_levels": id_levels,
        "db_name": DB_NAME,
    }
    return _templates.TemplateResponse(template_name, ctx)


# ── Project pages ─────────────────────────────────────────────────────────────

@router.get("/projects", response_class=HTMLResponse)
async def demo_projects(
    request: Request,
    page: int = 1,
    page_size: int = 25,
    search: str = "",
):
    engine = _get_engine()
    where_parts = []
    params: dict = {}
    if search:
        where_parts.append("(p.name LIKE :search OR p.description LIKE :search)")
        params["search"] = f"%{search}%"
    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    with engine.connect() as conn:
        total = conn.execute(
            text(f"SELECT COUNT(*) FROM project p {where_sql}"), params
        ).scalar()

        offset = (page - 1) * page_size
        rows = conn.execute(text(f"""
            SELECT p.id, p.name, p.description, p.date, p.access, p.rare_disease_focus,
                   COUNT(DISTINCT pe.to_id) AS experiment_count,
                   COUNT(DISTINCT pb.to_id) AS biosample_count
            FROM project p
            LEFT JOIN project_to_experiment pe ON pe.from_id = p.id
            LEFT JOIN project_to_biosample  pb ON pb.from_id = p.id
            {where_sql}
            GROUP BY p.id
            ORDER BY p.date DESC
            LIMIT :limit OFFSET :offset
        """), {**params, "limit": page_size, "offset": offset}).mappings().all()

    total_pages = max(1, (total + page_size - 1) // page_size)
    htmx = request.headers.get("HX-Request") == "true"
    template_name = "demo_projects_rows.html" if htmx else "demo_projects.html"
    ctx = {
        "request": request,
        "projects": [dict(r) for r in rows],
        "total": total, "page": page, "page_size": page_size,
        "total_pages": total_pages, "search": search, "db_name": DB_NAME,
    }
    return _templates.TemplateResponse(template_name, ctx)


@router.get("/projects/{project_id:path}", response_class=HTMLResponse)
async def demo_project_detail(request: Request, project_id: str):
    engine = _get_engine()
    with engine.connect() as conn:
        project = conn.execute(
            text("SELECT * FROM project WHERE id = :id"), {"id": project_id}
        ).mappings().first()
        if not project:
            return _templates.TemplateResponse("demo_not_found.html",
                {"request": request, "entity": "Project", "id": project_id})

        def _scalars(query, params=None):
            try:
                return conn.execute(text(query), params or {}).scalars().all()
            except Exception:
                return []

        lab_groups    = _scalars("SELECT value FROM project__lab_groups   WHERE id = :id", {"id": project_id})
        keywords      = _scalars("SELECT value FROM project__keywords     WHERE id = :id", {"id": project_id})
        project_types = _scalars("SELECT value FROM project__project_type WHERE id = :id", {"id": project_id})

        try:
            people = conn.execute(text("""
                SELECT p.name, p.email, pp.role
                FROM person p JOIN project_to_person pp ON pp.to_id = p.id
                WHERE pp.from_id = :pid
            """), {"pid": project_id}).mappings().all()
        except Exception:
            people = []

        experiments = conn.execute(text("""
            SELECT e.id, e.name, e.experiment_type, e.platform_type, e.date,
                   COUNT(DISTINCT ed.to_id) AS dataset_count,
                   COUNT(DISTINCT es.to_id) AS stats_count
            FROM experiment e
            JOIN project_to_experiment pe ON pe.to_id = e.id
            LEFT JOIN experiment_to_dataset      ed ON ed.from_id = e.id
            LEFT JOIN experiment_to_stats_result es ON es.from_id = e.id
            WHERE pe.from_id = :pid
            GROUP BY e.id ORDER BY e.date DESC
        """), {"pid": project_id}).mappings().all()

        biosample_count = conn.execute(
            text("SELECT COUNT(*) FROM project_to_biosample WHERE from_id = :id"),
            {"id": project_id}
        ).scalar()

    ctx = {
        "request": request, "project": dict(project),
        "lab_groups": lab_groups, "keywords": keywords, "project_types": project_types,
        "people": [dict(p) for p in people],
        "experiments": [dict(e) for e in experiments],
        "biosample_count": biosample_count, "db_name": DB_NAME,
    }
    return _templates.TemplateResponse("demo_project_detail.html", ctx)


@router.get("/experiments/{experiment_id:path}", response_class=HTMLResponse)
async def demo_experiment_detail(request: Request, experiment_id: str):
    engine = _get_engine()
    with engine.connect() as conn:
        experiment = conn.execute(
            text("SELECT * FROM experiment WHERE id = :id"), {"id": experiment_id}
        ).mappings().first()
        if not experiment:
            return _templates.TemplateResponse("demo_not_found.html",
                {"request": request, "entity": "Experiment", "id": experiment_id})

        project = conn.execute(text("""
            SELECT p.id, p.name FROM project p
            JOIN project_to_experiment pe ON pe.from_id = p.id
            WHERE pe.to_id = :eid LIMIT 1
        """), {"eid": experiment_id}).mappings().first()

        datasets = conn.execute(text("""
            SELECT d.id, d.data_type, d.row_count, d.column_count, d.file_reference
            FROM dataset d
            JOIN experiment_to_dataset ed ON ed.to_id = d.id
            WHERE ed.from_id = :eid
            ORDER BY d.data_type
        """), {"eid": experiment_id}).mappings().all()

        stats_results = conn.execute(text("""
            SELECT sr.id, sr.name, sr.data_type, sr.row_count, sr.column_count
            FROM stats_result sr
            JOIN experiment_to_stats_result es ON es.to_id = sr.id
            WHERE es.from_id = :eid
            ORDER BY sr.name
        """), {"eid": experiment_id}).mappings().all()

        try:
            people = conn.execute(text("""
                SELECT p.name, p.email, ep.role
                FROM person p JOIN experiment_to_person ep ON ep.to_id = p.id
                WHERE ep.from_id = :eid
            """), {"eid": experiment_id}).mappings().all()
        except Exception:
            people = []

    ctx = {
        "request": request, "experiment": dict(experiment),
        "project": dict(project) if project else None,
        "datasets": [dict(d) for d in datasets],
        "stats_results": [dict(s) for s in stats_results],
        "people": [dict(p) for p in people],
        "db_name": DB_NAME,
    }
    return _templates.TemplateResponse("demo_experiment_detail.html", ctx)


# ── Analyte detail pages ──────────────────────────────────────────────────────

@router.get("/metabolites/{metabolite_id:path}", response_class=HTMLResponse)
async def demo_metabolite_detail(request: Request, metabolite_id: str):
    engine = _get_engine()
    with engine.connect() as conn:
        metabolite = conn.execute(
            text("SELECT * FROM metabolite WHERE id = :id"), {"id": metabolite_id}
        ).mappings().first()
        if not metabolite:
            return _templates.TemplateResponse("demo_not_found.html",
                {"request": request, "entity": "Metabolite", "id": metabolite_id})

        try:
            synonyms = conn.execute(
                text("SELECT value FROM metabolite__synonyms WHERE id = :id"),
                {"id": metabolite_id}
            ).scalars().all()
        except Exception:
            synonyms = []

        experiments = conn.execute(text("""
            SELECT DISTINCT e.id AS exp_id, e.name AS exp_name, e.experiment_type,
                   p.id AS proj_id, p.name AS proj_name,
                   d.id AS dataset_id, d.data_type, d.row_count
            FROM measured_metabolite_to_metabolite mm2m
            JOIN measured_metabolite_dataset__data mmd
                ON mmd.measured_metabolite_id = mm2m.from_id
            JOIN dataset d ON d.id = mmd.dataset_id
            JOIN experiment_to_dataset ed ON ed.to_id = d.id
            JOIN experiment e ON e.id = ed.from_id
            JOIN project_to_experiment pe ON pe.to_id = e.id
            JOIN project p ON p.id = pe.from_id
            WHERE mm2m.to_id = :mid
            ORDER BY p.name, e.name, d.data_type
        """), {"mid": metabolite_id}).mappings().all()

        stats = conn.execute(text("""
            SELECT DISTINCT sr.id AS sr_id, sr.name AS sr_name, sr.data_type,
                   e.id AS exp_id, e.name AS exp_name, p.name AS proj_name
            FROM measured_metabolite_to_metabolite mm2m
            JOIN measured_metabolite_stats_result__data mmsr
                ON mmsr.measured_metabolite_id = mm2m.from_id
            JOIN stats_result sr ON sr.id = mmsr.stats_result_id
            JOIN experiment_to_stats_result es ON es.to_id = sr.id
            JOIN experiment e ON e.id = es.from_id
            JOIN project_to_experiment pe ON pe.to_id = e.id
            JOIN project p ON p.id = pe.from_id
            WHERE mm2m.to_id = :mid
            ORDER BY p.name, e.name
        """), {"mid": metabolite_id}).mappings().all()

        pathways = conn.execute(text("""
            SELECT pw.id, pw.name, pw.type, pw.category, pw.source_id, m2p.source
            FROM metabolite_to_pathway m2p
            JOIN pathway pw ON pw.id = m2p.to_id
            WHERE m2p.from_id = :mid
            ORDER BY pw.name
        """), {"mid": metabolite_id}).mappings().all()

    ctx = {
        "request": request, "metabolite": dict(metabolite),
        "synonyms": list(synonyms),
        "experiments": [dict(r) for r in experiments],
        "stats": [dict(r) for r in stats],
        "pathways": [dict(r) for r in pathways],
        "db_name": DB_NAME,
    }
    return _templates.TemplateResponse("demo_metabolite_detail.html", ctx)


@router.get("/genes/{gene_id:path}", response_class=HTMLResponse)
async def demo_gene_detail(request: Request, gene_id: str):
    engine = _get_engine()
    with engine.connect() as conn:
        gene = conn.execute(
            text("SELECT * FROM gene WHERE id = :id"), {"id": gene_id}
        ).mappings().first()
        if not gene:
            return _templates.TemplateResponse("demo_not_found.html",
                {"request": request, "entity": "Gene", "id": gene_id})

        experiments = conn.execute(text("""
            SELECT DISTINCT e.id AS exp_id, e.name AS exp_name, e.experiment_type,
                   p.id AS proj_id, p.name AS proj_name,
                   d.id AS dataset_id, d.data_type, d.row_count
            FROM measured_gene_to_gene mg2g
            JOIN measured_gene_dataset__data mgd
                ON mgd.measured_gene_id = mg2g.from_id
            JOIN dataset d ON d.id = mgd.dataset_id
            JOIN experiment_to_dataset ed ON ed.to_id = d.id
            JOIN experiment e ON e.id = ed.from_id
            JOIN project_to_experiment pe ON pe.to_id = e.id
            JOIN project p ON p.id = pe.from_id
            WHERE mg2g.to_id = :gid
            ORDER BY p.name, e.name, d.data_type
        """), {"gid": gene_id}).mappings().all()

        stats = conn.execute(text("""
            SELECT DISTINCT sr.id AS sr_id, sr.name AS sr_name,
                   e.id AS exp_id, e.name AS exp_name, p.name AS proj_name
            FROM measured_gene_to_gene mg2g
            JOIN measured_gene_stats_result__data mgsr
                ON mgsr.measured_gene_id = mg2g.from_id
            JOIN stats_result sr ON sr.id = mgsr.stats_result_id
            JOIN experiment_to_stats_result es ON es.to_id = sr.id
            JOIN experiment e ON e.id = es.from_id
            JOIN project_to_experiment pe ON pe.to_id = e.id
            JOIN project p ON p.id = pe.from_id
            WHERE mg2g.to_id = :gid
            ORDER BY p.name, e.name
        """), {"gid": gene_id}).mappings().all()

    ctx = {
        "request": request, "gene": dict(gene),
        "experiments": [dict(r) for r in experiments],
        "stats": [dict(r) for r in stats],
        "db_name": DB_NAME,
    }
    return _templates.TemplateResponse("demo_gene_detail.html", ctx)


# ── Pathway pages ─────────────────────────────────────────────────────────────

@router.get("/pathways", response_class=HTMLResponse)
async def demo_pathways(
    request: Request,
    page: int = 1,
    page_size: int = 50,
    search: str = "",
    pw_type: str = "",
    sort_by: str = "measured",   # "name" | "measured"
):
    engine = _get_engine()
    where_parts = []
    params: dict = {}
    if search:
        where_parts.append("pw.name LIKE :search")
        params["search"] = f"%{search}%"
    if pw_type:
        where_parts.append("pw.type = :pw_type")
        params["pw_type"] = pw_type
    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    with engine.connect() as conn:
        total = conn.execute(
            text(f"SELECT COUNT(DISTINCT pw.id) FROM pathway pw {where_sql}"), params
        ).scalar()

        pw_types = _cached("pw_types", lambda: [
            {"type": r["type"] or "unknown", "count": r["cnt"]}
            for r in conn.execute(text(
                "SELECT type, COUNT(*) AS cnt FROM pathway GROUP BY type ORDER BY cnt DESC"
            )).mappings().all()
        ])

        # Cache expensive full-table aggregations — counts are stable between ETL runs.
        metabolite_counts: dict = _cached("pw_metabolite_counts", lambda: {
            r["pw_id"]: (r["metabolite_count"], r["metabolites_with_data"])
            for r in conn.execute(text("""
                SELECT m2p.to_id AS pw_id,
                       COUNT(DISTINCT m2p.from_id) AS metabolite_count,
                       COUNT(DISTINCT CASE WHEN mm2m.from_id IS NOT NULL
                                          THEN m2p.from_id END) AS metabolites_with_data
                FROM metabolite_to_pathway m2p
                LEFT JOIN measured_metabolite_to_metabolite mm2m ON mm2m.to_id = m2p.from_id
                GROUP BY m2p.to_id
            """)).mappings().all()
        })

        gene_counts: dict = _cached("pw_gene_counts", lambda: {
            r["pw_id"]: (r["gene_count"], r["genes_with_data"])
            for r in conn.execute(text("""
                SELECT g2p.to_id AS pw_id,
                       COUNT(DISTINCT g2p.from_id) AS gene_count,
                       COUNT(DISTINCT CASE WHEN mg2g.from_id IS NOT NULL
                                          THEN g2p.from_id END) AS genes_with_data
                FROM gene_to_pathway g2p
                LEFT JOIN measured_gene_to_gene mg2g ON mg2g.to_id = g2p.from_id
                GROUP BY g2p.to_id
            """)).mappings().all()
        })

        if sort_by == "name":
            offset = (page - 1) * page_size
            raw_rows = conn.execute(text(f"""
                SELECT pw.id, pw.name, pw.type, pw.category, pw.source_id
                FROM pathway pw
                {where_sql}
                ORDER BY pw.name
                LIMIT :limit OFFSET :offset
            """), {**params, "limit": page_size, "offset": offset}).mappings().all()
        else:
            # Fetch all matching rows; sort by count in Python after merging cache.
            raw_rows = conn.execute(text(f"""
                SELECT pw.id, pw.name, pw.type, pw.category, pw.source_id
                FROM pathway pw
                {where_sql}
                ORDER BY pw.name
            """), params).mappings().all()

    rows = []
    for pw in raw_rows:
        r = dict(pw)
        mc = metabolite_counts.get(r["id"], (0, 0))
        gc = gene_counts.get(r["id"], (0, 0))
        r["metabolite_count"] = mc[0]
        r["metabolites_with_data"] = mc[1]
        r["gene_count"] = gc[0]
        r["genes_with_data"] = gc[1]
        rows.append(r)

    if sort_by != "name":
        rows.sort(key=lambda r: (-r["metabolites_with_data"], r["name"] or ""))
        offset = (page - 1) * page_size
        rows = rows[offset: offset + page_size]

    total_pages = max(1, (total + page_size - 1) // page_size)
    htmx = request.headers.get("HX-Request") == "true"
    template_name = "demo_pathways_rows.html" if htmx else "demo_pathways.html"
    ctx = {
        "request": request,
        "pathways": rows,
        "total": total, "page": page, "page_size": page_size,
        "total_pages": total_pages, "search": search,
        "pw_type": pw_type, "pw_types": pw_types, "sort_by": sort_by, "db_name": DB_NAME,
    }
    return _templates.TemplateResponse(template_name, ctx)


@router.get("/pathways/{pathway_id:path}", response_class=HTMLResponse)
async def demo_pathway_detail(request: Request, pathway_id: str):
    engine = _get_engine()
    loop = asyncio.get_running_loop()

    def _q(sql, params=None):
        with engine.connect() as conn:
            return conn.execute(text(sql), params or {}).mappings().all()

    def _q_safe(sql, params=None):
        try:
            return _q(sql, params)
        except Exception:
            return []

    # Check pathway exists first.
    pw_rows = await loop.run_in_executor(None, _q, "SELECT * FROM pathway WHERE id = :id", {"id": pathway_id})
    if not pw_rows:
        return _templates.TemplateResponse("demo_not_found.html",
            {"request": request, "entity": "Pathway", "id": pathway_id})
    pathway = pw_rows[0]

    # Run all remaining queries in parallel — each gets its own connection.
    (
        _met_rows,
        _exp_agg,
        _sr_agg,
        dataset_rows,
        stats_rows,
        _gene_rows,
        _g_exp_agg,
        _g_sr_agg,
        gene_dataset_rows,
        gene_stats_rows,
    ) = await asyncio.gather(
        loop.run_in_executor(None, _q, """
            SELECT met.id, met.name
            FROM metabolite_to_pathway m2p
            JOIN metabolite met ON met.id = m2p.from_id
            WHERE m2p.to_id = :pid
            ORDER BY met.name
        """, {"pid": pathway_id}),
        loop.run_in_executor(None, _q, """
            SELECT mm2m.to_id AS met_id,
                   COUNT(DISTINCT ed.from_id) AS experiment_count,
                   GROUP_CONCAT(DISTINCT e.name ORDER BY e.name SEPARATOR ' · ') AS experiment_names
            FROM metabolite_to_pathway m2p
            JOIN measured_metabolite_to_metabolite mm2m ON mm2m.to_id = m2p.from_id
            JOIN measured_metabolite_dataset__data mmd ON mmd.measured_metabolite_id = mm2m.from_id
            JOIN experiment_to_dataset ed ON ed.to_id = mmd.dataset_id
            JOIN experiment e ON e.id = ed.from_id
            WHERE m2p.to_id = :pid
            GROUP BY mm2m.to_id
        """, {"pid": pathway_id}),
        loop.run_in_executor(None, _q, """
            SELECT mm2m.to_id AS met_id,
                   COUNT(DISTINCT mmsr.stats_result_id) AS stats_count
            FROM metabolite_to_pathway m2p
            JOIN measured_metabolite_to_metabolite mm2m ON mm2m.to_id = m2p.from_id
            JOIN measured_metabolite_stats_result__data mmsr ON mmsr.measured_metabolite_id = mm2m.from_id
            WHERE m2p.to_id = :pid
            GROUP BY mm2m.to_id
        """, {"pid": pathway_id}),
        loop.run_in_executor(None, _q, """
            SELECT DISTINCT
                p.id AS proj_id, p.name AS proj_name,
                e.id AS exp_id, e.name AS exp_name, e.experiment_type,
                d.id AS dataset_id, d.data_type, d.row_count
            FROM metabolite_to_pathway m2p
            JOIN metabolite met ON met.id = m2p.from_id
            JOIN measured_metabolite_to_metabolite mm2m ON mm2m.to_id = met.id
            JOIN measured_metabolite_dataset__data mmd ON mmd.measured_metabolite_id = mm2m.from_id
            JOIN dataset d ON d.id = mmd.dataset_id
            JOIN experiment_to_dataset ed ON ed.to_id = d.id
            JOIN experiment e ON e.id = ed.from_id
            JOIN project_to_experiment pe ON pe.to_id = e.id
            JOIN project p ON p.id = pe.from_id
            WHERE m2p.to_id = :pid
            ORDER BY p.name, e.name, d.data_type
        """, {"pid": pathway_id}),
        loop.run_in_executor(None, _q, """
            SELECT DISTINCT
                p.id AS proj_id, p.name AS proj_name,
                e.id AS exp_id, e.name AS exp_name,
                sr.id AS sr_id, sr.name AS sr_name, sr.data_type
            FROM metabolite_to_pathway m2p
            JOIN metabolite met ON met.id = m2p.from_id
            JOIN measured_metabolite_to_metabolite mm2m ON mm2m.to_id = met.id
            JOIN measured_metabolite_stats_result__data mmsr ON mmsr.measured_metabolite_id = mm2m.from_id
            JOIN stats_result sr ON sr.id = mmsr.stats_result_id
            JOIN experiment_to_stats_result es ON es.to_id = sr.id
            JOIN experiment e ON e.id = es.from_id
            JOIN project_to_experiment pe ON pe.to_id = e.id
            JOIN project p ON p.id = pe.from_id
            WHERE m2p.to_id = :pid
            ORDER BY p.name, e.name, sr.name
        """, {"pid": pathway_id}),
        loop.run_in_executor(None, _q_safe, """
            SELECT g.id, COALESCE(g.symbol, g.id) AS name
            FROM gene_to_pathway g2p
            JOIN gene g ON g.id = g2p.from_id
            WHERE g2p.to_id = :pid
            ORDER BY name
        """, {"pid": pathway_id}),
        loop.run_in_executor(None, _q_safe, """
            SELECT mg2g.to_id AS gene_id,
                   COUNT(DISTINCT ed.from_id) AS experiment_count,
                   GROUP_CONCAT(DISTINCT e.name ORDER BY e.name SEPARATOR ' · ') AS experiment_names
            FROM gene_to_pathway g2p
            JOIN measured_gene_to_gene mg2g ON mg2g.to_id = g2p.from_id
            JOIN measured_gene_dataset__data mgd ON mgd.measured_gene_id = mg2g.from_id
            JOIN experiment_to_dataset ed ON ed.to_id = mgd.dataset_id
            JOIN experiment e ON e.id = ed.from_id
            WHERE g2p.to_id = :pid
            GROUP BY mg2g.to_id
        """, {"pid": pathway_id}),
        loop.run_in_executor(None, _q_safe, """
            SELECT mg2g.to_id AS gene_id,
                   COUNT(DISTINCT mgsr.stats_result_id) AS stats_count
            FROM gene_to_pathway g2p
            JOIN measured_gene_to_gene mg2g ON mg2g.to_id = g2p.from_id
            JOIN measured_gene_stats_result__data mgsr ON mgsr.measured_gene_id = mg2g.from_id
            WHERE g2p.to_id = :pid
            GROUP BY mg2g.to_id
        """, {"pid": pathway_id}),
        loop.run_in_executor(None, _q_safe, """
            SELECT DISTINCT
                p.id AS proj_id, p.name AS proj_name,
                e.id AS exp_id, e.name AS exp_name, e.experiment_type,
                d.id AS dataset_id, d.data_type, d.row_count
            FROM gene_to_pathway g2p
            JOIN gene g ON g.id = g2p.from_id
            JOIN measured_gene_to_gene mg2g ON mg2g.to_id = g.id
            JOIN measured_gene_dataset__data mgd ON mgd.measured_gene_id = mg2g.from_id
            JOIN dataset d ON d.id = mgd.dataset_id
            JOIN experiment_to_dataset ed ON ed.to_id = d.id
            JOIN experiment e ON e.id = ed.from_id
            JOIN project_to_experiment pe ON pe.to_id = e.id
            JOIN project p ON p.id = pe.from_id
            WHERE g2p.to_id = :pid
            ORDER BY p.name, e.name, d.data_type
        """, {"pid": pathway_id}),
        loop.run_in_executor(None, _q_safe, """
            SELECT DISTINCT
                p.id AS proj_id, p.name AS proj_name,
                e.id AS exp_id, e.name AS exp_name,
                sr.id AS sr_id, sr.name AS sr_name, sr.data_type
            FROM gene_to_pathway g2p
            JOIN gene g ON g.id = g2p.from_id
            JOIN measured_gene_to_gene mg2g ON mg2g.to_id = g.id
            JOIN measured_gene_stats_result__data mgsr ON mgsr.measured_gene_id = mg2g.from_id
            JOIN stats_result sr ON sr.id = mgsr.stats_result_id
            JOIN experiment_to_stats_result es ON es.to_id = sr.id
            JOIN experiment e ON e.id = es.from_id
            JOIN project_to_experiment pe ON pe.to_id = e.id
            JOIN project p ON p.id = pe.from_id
            WHERE g2p.to_id = :pid
            ORDER BY p.name, e.name, sr.name
        """, {"pid": pathway_id}),
    )

    _exp_map = {r["met_id"]: r for r in _exp_agg}
    _sr_map  = {r["met_id"]: r for r in _sr_agg}
    coverage = sorted([
        {
            "id":               r["id"],
            "name":             r["name"],
            "experiment_count": _exp_map.get(r["id"], {}).get("experiment_count", 0),
            "experiment_names": _exp_map.get(r["id"], {}).get("experiment_names", ""),
            "stats_count":      _sr_map.get(r["id"],  {}).get("stats_count", 0),
        }
        for r in _met_rows
    ], key=lambda x: (-x["experiment_count"], x["name"] or ""))

    _g_exp_map = {r["gene_id"]: r for r in _g_exp_agg}
    _g_sr_map  = {r["gene_id"]: r for r in _g_sr_agg}
    gene_coverage = sorted([
        {
            "id":               r["id"],
            "name":             r["name"],
            "experiment_count": _g_exp_map.get(r["id"], {}).get("experiment_count", 0),
            "experiment_names": _g_exp_map.get(r["id"], {}).get("experiment_names", ""),
            "stats_count":      _g_sr_map.get(r["id"],  {}).get("stats_count", 0),
        }
        for r in _gene_rows
    ], key=lambda x: (-x["experiment_count"], x["name"] or ""))

    # Merge datasets and stats_results into experiments list keyed by exp_id
    experiments_map: dict = {}
    for row in dataset_rows:
        exp_id = row["exp_id"]
        if exp_id not in experiments_map:
            experiments_map[exp_id] = {
                "exp_id": exp_id,
                "exp_name": row["exp_name"],
                "proj_id": row["proj_id"],
                "proj_name": row["proj_name"],
                "experiment_type": row["experiment_type"],
                "datasets": [],
                "stats_results": [],
            }
        experiments_map[exp_id]["datasets"].append({
            "dataset_id": row["dataset_id"],
            "data_type": row["data_type"],
            "row_count": row["row_count"],
        })
    for row in stats_rows:
        exp_id = row["exp_id"]
        if exp_id not in experiments_map:
            experiments_map[exp_id] = {
                "exp_id": exp_id,
                "exp_name": row["exp_name"],
                "proj_id": row["proj_id"],
                "proj_name": row["proj_name"],
                "experiment_type": None,
                "datasets": [],
                "stats_results": [],
            }
        experiments_map[exp_id]["stats_results"].append({
            "sr_id": row["sr_id"],
            "sr_name": row["sr_name"],
            "data_type": row["data_type"],
        })

    experiments = list(experiments_map.values())

    # Build gene_experiments map
    gene_experiments_map: dict = {}
    for row in gene_dataset_rows:
        exp_id = row["exp_id"]
        if exp_id not in gene_experiments_map:
            gene_experiments_map[exp_id] = {
                "exp_id": exp_id,
                "exp_name": row["exp_name"],
                "proj_id": row["proj_id"],
                "proj_name": row["proj_name"],
                "experiment_type": row["experiment_type"],
                "datasets": [],
                "stats_results": [],
            }
        gene_experiments_map[exp_id]["datasets"].append({
            "dataset_id": row["dataset_id"],
            "data_type": row["data_type"],
            "row_count": row["row_count"],
        })
    for row in gene_stats_rows:
        exp_id = row["exp_id"]
        if exp_id not in gene_experiments_map:
            gene_experiments_map[exp_id] = {
                "exp_id": exp_id,
                "exp_name": row["exp_name"],
                "proj_id": row["proj_id"],
                "proj_name": row["proj_name"],
                "experiment_type": None,
                "datasets": [],
                "stats_results": [],
            }
        gene_experiments_map[exp_id]["stats_results"].append({
            "sr_id": row["sr_id"],
            "sr_name": row["sr_name"],
            "data_type": row["data_type"],
        })
    gene_experiments = list(gene_experiments_map.values())

    ctx = {
        "request": request, "pathway": dict(pathway),
        "coverage": [dict(r) for r in coverage],
        "experiments": experiments,
        "gene_coverage": [dict(r) for r in gene_coverage],
        "gene_experiments": gene_experiments,
        "db_name": DB_NAME,
    }
    return _templates.TemplateResponse("demo_pathway_detail.html", ctx)


@router.get("/dataset-preview", response_class=HTMLResponse)
async def demo_dataset_preview(request: Request, dataset_id: str, pathway_id: str):
    engine = _get_engine()
    with engine.connect() as conn:
        total_row_count = conn.execute(
            text("SELECT row_count FROM dataset WHERE id = :id"), {"id": dataset_id}
        ).scalar() or 0

        data_rows = conn.execute(text("""
            SELECT mmd.measured_metabolite_id, mmd.run_biosample_id, mmd.value,
                   met.id AS met_id, COALESCE(met.name, met.id) AS met_name
            FROM measured_metabolite_dataset__data mmd
            JOIN measured_metabolite_to_metabolite mm2m ON mm2m.from_id = mmd.measured_metabolite_id
            JOIN metabolite met ON met.id = mm2m.to_id
            JOIN metabolite_to_pathway m2p ON m2p.from_id = met.id
            WHERE mmd.dataset_id = :dataset_id AND m2p.to_id = :pathway_id
            ORDER BY met_name, mmd.run_biosample_id
        """), {"dataset_id": dataset_id, "pathway_id": pathway_id}).mappings().all()

        seen_cols: dict = {}
        for row in data_rows:
            rb_id = row["run_biosample_id"]
            if rb_id not in seen_cols:
                seen_cols[rb_id] = True
        rb_ids = list(seen_cols.keys())

        condition_groups, col_order, meta_panel_rows = _fetch_column_metadata(conn, rb_ids)

    matrix: dict = {}
    for row in data_rows:
        key = (row["met_id"], row["met_name"])
        if key not in matrix:
            matrix[key] = {}
        val = row["value"]
        if val is not None:
            try:
                matrix[key][row["run_biosample_id"]] = f"{float(val):.3g}"
            except (TypeError, ValueError):
                matrix[key][row["run_biosample_id"]] = str(val)
        else:
            matrix[key][row["run_biosample_id"]] = None

    rows = [(met_id, met_name, vals) for (met_id, met_name), vals in matrix.items()]

    return _templates.TemplateResponse("demo_dataset_preview.html", {
        "request": request,
        "condition_groups": condition_groups,
        "col_order": col_order,
        "rows": rows,
        "meta_panel_rows": meta_panel_rows,
        "dataset_id": dataset_id,
        "pathway_analyte_count": len(rows),
        "analyte_label": "Metabolite",
        "analyte_url_prefix": "/demo/metabolites/",
        "total_row_count": total_row_count,
    })


@router.get("/stats-preview", response_class=HTMLResponse)
async def demo_stats_preview(request: Request, stats_result_id: str, pathway_id: str):
    engine = _get_engine()
    with engine.connect() as conn:
        total_row_count = conn.execute(
            text("SELECT row_count FROM stats_result WHERE id = :id"), {"id": stats_result_id}
        ).scalar() or 0

        col_rows = conn.execute(text("""
            SELECT column_name, stat_type, group1, group2, comparison, notes
            FROM stats_result__comparison_columns
            WHERE parent_id = :sr_id
            ORDER BY id
        """), {"sr_id": stats_result_id}).mappings().all()

        data_rows = conn.execute(text("""
            SELECT mmsr.measured_metabolite_id, mmsr.column_name, mmsr.value,
                   met.id AS met_id, COALESCE(met.name, met.id) AS met_name
            FROM measured_metabolite_stats_result__data mmsr
            JOIN measured_metabolite_to_metabolite mm2m ON mm2m.from_id = mmsr.measured_metabolite_id
            JOIN metabolite met ON met.id = mm2m.to_id
            JOIN metabolite_to_pathway m2p ON m2p.from_id = met.id
            WHERE mmsr.stats_result_id = :sr_id AND m2p.to_id = :pathway_id
            ORDER BY met_name, mmsr.column_name
        """), {"sr_id": stats_result_id, "pathway_id": pathway_id}).mappings().all()

    # Build comparisons list preserving insertion order
    comparisons_map: dict = {}
    col_order = []
    for row in col_rows:
        comp = row["comparison"] or ""
        if comp not in comparisons_map:
            comparisons_map[comp] = {
                "comparison": comp,
                "group1": row["group1"] or "",
                "group2": row["group2"] or "",
                "columns": [],
            }
        col_entry = {
            "column_name": row["column_name"],
            "stat_type": row["stat_type"] or "",
            "notes": row["notes"] or "",
        }
        comparisons_map[comp]["columns"].append(col_entry)
        col_order.append(row["column_name"])

    comparisons = list(comparisons_map.values())

    # Pivot data: keyed by (met_id, met_name) → {column_name: value}
    matrix: dict = {}
    for row in data_rows:
        key = (row["met_id"], row["met_name"])
        if key not in matrix:
            matrix[key] = {}
        val = row["value"]
        if val is not None:
            try:
                matrix[key][row["column_name"]] = f"{float(val):.3g}"
            except (TypeError, ValueError):
                matrix[key][row["column_name"]] = str(val)
        else:
            matrix[key][row["column_name"]] = None

    rows = [(met_id, met_name, vals) for (met_id, met_name), vals in matrix.items()]

    return _templates.TemplateResponse("demo_stats_preview.html", {
        "request": request,
        "comparisons": comparisons,
        "col_order": col_order,
        "rows": rows,
        "stats_result_id": stats_result_id,
        "pathway_analyte_count": len(rows),
        "analyte_label": "Metabolite",
        "analyte_url_prefix": "/demo/metabolites/",
        "total_row_count": total_row_count,
    })


@router.get("/gene-dataset-preview", response_class=HTMLResponse)
async def demo_gene_dataset_preview(request: Request, dataset_id: str, pathway_id: str):
    engine = _get_engine()
    with engine.connect() as conn:
        total_row_count = conn.execute(
            text("SELECT row_count FROM dataset WHERE id = :id"), {"id": dataset_id}
        ).scalar() or 0

        data_rows = conn.execute(text("""
            SELECT mgd.measured_gene_id, mgd.run_biosample_id, mgd.value,
                   g.id AS gene_id, COALESCE(g.symbol, g.id) AS gene_name
            FROM measured_gene_dataset__data mgd
            JOIN measured_gene_to_gene mg2g ON mg2g.from_id = mgd.measured_gene_id
            JOIN gene g ON g.id = mg2g.to_id
            JOIN gene_to_pathway g2p ON g2p.from_id = g.id
            WHERE mgd.dataset_id = :dataset_id AND g2p.to_id = :pathway_id
            ORDER BY gene_name, mgd.run_biosample_id
        """), {"dataset_id": dataset_id, "pathway_id": pathway_id}).mappings().all()

        seen_cols: dict = {}
        for row in data_rows:
            rb_id = row["run_biosample_id"]
            if rb_id not in seen_cols:
                seen_cols[rb_id] = True
        rb_ids = list(seen_cols.keys())

        condition_groups, col_order, meta_panel_rows = _fetch_column_metadata(conn, rb_ids)

    matrix: dict = {}
    for row in data_rows:
        key = (row["gene_id"], row["gene_name"])
        if key not in matrix:
            matrix[key] = {}
        val = row["value"]
        if val is not None:
            try:
                matrix[key][row["run_biosample_id"]] = f"{float(val):.3g}"
            except (TypeError, ValueError):
                matrix[key][row["run_biosample_id"]] = str(val)
        else:
            matrix[key][row["run_biosample_id"]] = None

    rows = [(gene_id, gene_name, vals) for (gene_id, gene_name), vals in matrix.items()]

    return _templates.TemplateResponse("demo_dataset_preview.html", {
        "request": request,
        "condition_groups": condition_groups,
        "col_order": col_order,
        "rows": rows,
        "meta_panel_rows": meta_panel_rows,
        "dataset_id": dataset_id,
        "pathway_analyte_count": len(rows),
        "analyte_label": "Gene",
        "analyte_url_prefix": "/demo/genes/",
        "total_row_count": total_row_count,
    })


@router.get("/gene-stats-preview", response_class=HTMLResponse)
async def demo_gene_stats_preview(request: Request, stats_result_id: str, pathway_id: str):
    engine = _get_engine()
    with engine.connect() as conn:
        total_row_count = conn.execute(
            text("SELECT row_count FROM stats_result WHERE id = :id"), {"id": stats_result_id}
        ).scalar() or 0

        col_rows = conn.execute(text("""
            SELECT column_name, stat_type, group1, group2, comparison, notes
            FROM stats_result__comparison_columns
            WHERE parent_id = :sr_id
            ORDER BY id
        """), {"sr_id": stats_result_id}).mappings().all()

        data_rows = conn.execute(text("""
            SELECT mgsr.measured_gene_id, mgsr.column_name, mgsr.value,
                   g.id AS gene_id, COALESCE(g.symbol, g.id) AS gene_name
            FROM measured_gene_stats_result__data mgsr
            JOIN measured_gene_to_gene mg2g ON mg2g.from_id = mgsr.measured_gene_id
            JOIN gene g ON g.id = mg2g.to_id
            JOIN gene_to_pathway g2p ON g2p.from_id = g.id
            WHERE mgsr.stats_result_id = :sr_id AND g2p.to_id = :pathway_id
            ORDER BY gene_name, mgsr.column_name
        """), {"sr_id": stats_result_id, "pathway_id": pathway_id}).mappings().all()

    comparisons_map: dict = {}
    col_order = []
    for row in col_rows:
        comp = row["comparison"] or ""
        if comp not in comparisons_map:
            comparisons_map[comp] = {
                "comparison": comp,
                "group1": row["group1"] or "",
                "group2": row["group2"] or "",
                "columns": [],
            }
        col_entry = {
            "column_name": row["column_name"],
            "stat_type": row["stat_type"] or "",
            "notes": row["notes"] or "",
        }
        comparisons_map[comp]["columns"].append(col_entry)
        col_order.append(row["column_name"])

    comparisons = list(comparisons_map.values())

    matrix: dict = {}
    for row in data_rows:
        key = (row["gene_id"], row["gene_name"])
        if key not in matrix:
            matrix[key] = {}
        val = row["value"]
        if val is not None:
            try:
                matrix[key][row["column_name"]] = f"{float(val):.3g}"
            except (TypeError, ValueError):
                matrix[key][row["column_name"]] = str(val)
        else:
            matrix[key][row["column_name"]] = None

    rows = [(gene_id, gene_name, vals) for (gene_id, gene_name), vals in matrix.items()]

    return _templates.TemplateResponse("demo_stats_preview.html", {
        "request": request,
        "comparisons": comparisons,
        "col_order": col_order,
        "rows": rows,
        "stats_result_id": stats_result_id,
        "pathway_analyte_count": len(rows),
        "analyte_label": "Gene",
        "analyte_url_prefix": "/demo/genes/",
        "total_row_count": total_row_count,
    })


# ── Metabolite Class pages ─────────────────────────────────────────────────────

@router.get("/metabolite-classes", response_class=HTMLResponse)
async def demo_metabolite_classes(
    request: Request,
    page: int = 1,
    page_size: int = 50,
    search: str = "",
    level: str = "",
    sort_by: str = "measured",   # "name" | "measured"
):
    engine = _get_engine()
    where_parts = []
    params: dict = {}
    if search:
        where_parts.append("mc.name LIKE :search")
        params["search"] = f"%{search}%"
    if level:
        where_parts.append("mc.level = :level")
        params["level"] = level
    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    with engine.connect() as conn:
        total = conn.execute(
            text(f"SELECT COUNT(DISTINCT mc.id) FROM metabolite_class mc {where_sql}"), params
        ).scalar()

        levels = _cached("mc_levels", lambda: [
            {"level": r["level"] or "unknown", "count": r["cnt"]}
            for r in conn.execute(text(
                "SELECT level, COUNT(*) AS cnt FROM metabolite_class "
                "GROUP BY level ORDER BY cnt DESC"
            )).mappings().all()
        ])

        # Cache expensive full-table aggregation — counts are stable between ETL runs.
        mc_counts: dict = _cached("mc_metabolite_counts", lambda: {
            r["class_id"]: (r["metabolite_count"], r["metabolites_with_data"])
            for r in conn.execute(text("""
                SELECT m2mc.to_id AS class_id,
                       COUNT(DISTINCT m2mc.from_id) AS metabolite_count,
                       COUNT(DISTINCT CASE WHEN mm2m.from_id IS NOT NULL
                                          THEN m2mc.from_id END) AS metabolites_with_data
                FROM metabolite_to_metabolite_class m2mc
                LEFT JOIN measured_metabolite_to_metabolite mm2m ON mm2m.to_id = m2mc.from_id
                GROUP BY m2mc.to_id
            """)).mappings().all()
        })

        if sort_by == "name":
            offset = (page - 1) * page_size
            raw_rows = conn.execute(text(f"""
                SELECT mc.id, mc.name, mc.level, mc.source
                FROM metabolite_class mc
                {where_sql}
                ORDER BY mc.name
                LIMIT :limit OFFSET :offset
            """), {**params, "limit": page_size, "offset": offset}).mappings().all()
        else:
            raw_rows = conn.execute(text(f"""
                SELECT mc.id, mc.name, mc.level, mc.source
                FROM metabolite_class mc
                {where_sql}
                ORDER BY mc.name
            """), params).mappings().all()

    rows = []
    for mc_row in raw_rows:
        r = dict(mc_row)
        counts = mc_counts.get(r["id"], (0, 0))
        r["metabolite_count"] = counts[0]
        r["metabolites_with_data"] = counts[1]
        rows.append(r)

    if sort_by != "name":
        rows.sort(key=lambda r: (-r["metabolites_with_data"], r["name"] or ""))
        offset = (page - 1) * page_size
        rows = rows[offset: offset + page_size]

    total_pages = max(1, (total + page_size - 1) // page_size)
    htmx = request.headers.get("HX-Request") == "true"
    template_name = "demo_metabolite_classes_rows.html" if htmx else "demo_metabolite_classes.html"
    ctx = {
        "request": request,
        "classes": rows,
        "total": total, "page": page, "page_size": page_size,
        "total_pages": total_pages, "search": search,
        "level": level, "levels": levels, "sort_by": sort_by, "db_name": DB_NAME,
    }
    return _templates.TemplateResponse(template_name, ctx)


@router.get("/metabolite-classes/{class_id:path}", response_class=HTMLResponse)
async def demo_metabolite_class_detail(request: Request, class_id: str):
    engine = _get_engine()
    with engine.connect() as conn:
        mc = conn.execute(
            text("SELECT * FROM metabolite_class WHERE id = :id"), {"id": class_id}
        ).mappings().first()
        if not mc:
            return _templates.TemplateResponse("demo_not_found.html",
                {"request": request, "entity": "Metabolite Class", "id": class_id})

        _met_rows = conn.execute(text("""
            SELECT met.id, COALESCE(met.name, met.id) AS name
            FROM metabolite_to_metabolite_class m2mc
            JOIN metabolite met ON met.id = m2mc.from_id
            WHERE m2mc.to_id = :cid
            ORDER BY name
        """), {"cid": class_id}).mappings().all()

        _exp_agg = conn.execute(text("""
            SELECT mm2m.to_id AS met_id,
                   COUNT(DISTINCT ed.from_id) AS experiment_count,
                   GROUP_CONCAT(DISTINCT e.name ORDER BY e.name SEPARATOR ' · ') AS experiment_names
            FROM metabolite_to_metabolite_class m2mc
            JOIN measured_metabolite_to_metabolite mm2m ON mm2m.to_id = m2mc.from_id
            JOIN measured_metabolite_dataset__data mmd ON mmd.measured_metabolite_id = mm2m.from_id
            JOIN experiment_to_dataset ed ON ed.to_id = mmd.dataset_id
            JOIN experiment e ON e.id = ed.from_id
            WHERE m2mc.to_id = :cid
            GROUP BY mm2m.to_id
        """), {"cid": class_id}).mappings().all()

        _sr_agg = conn.execute(text("""
            SELECT mm2m.to_id AS met_id,
                   COUNT(DISTINCT mmsr.stats_result_id) AS stats_count
            FROM metabolite_to_metabolite_class m2mc
            JOIN measured_metabolite_to_metabolite mm2m ON mm2m.to_id = m2mc.from_id
            JOIN measured_metabolite_stats_result__data mmsr ON mmsr.measured_metabolite_id = mm2m.from_id
            WHERE m2mc.to_id = :cid
            GROUP BY mm2m.to_id
        """), {"cid": class_id}).mappings().all()

        _exp_map = {r["met_id"]: r for r in _exp_agg}
        _sr_map  = {r["met_id"]: r for r in _sr_agg}
        coverage = sorted([
            {
                "id":               r["id"],
                "name":             r["name"],
                "experiment_count": _exp_map.get(r["id"], {}).get("experiment_count", 0),
                "experiment_names": _exp_map.get(r["id"], {}).get("experiment_names", ""),
                "stats_count":      _sr_map.get(r["id"],  {}).get("stats_count", 0),
            }
            for r in _met_rows
        ], key=lambda x: (-x["experiment_count"], x["name"] or ""))

        dataset_rows = conn.execute(text("""
            SELECT DISTINCT
                p.id AS proj_id, p.name AS proj_name,
                e.id AS exp_id, e.name AS exp_name, e.experiment_type,
                d.id AS dataset_id, d.data_type, d.row_count
            FROM metabolite_to_metabolite_class m2mc
            JOIN metabolite met ON met.id = m2mc.from_id
            JOIN measured_metabolite_to_metabolite mm2m ON mm2m.to_id = met.id
            JOIN measured_metabolite_dataset__data mmd ON mmd.measured_metabolite_id = mm2m.from_id
            JOIN dataset d ON d.id = mmd.dataset_id
            JOIN experiment_to_dataset ed ON ed.to_id = d.id
            JOIN experiment e ON e.id = ed.from_id
            JOIN project_to_experiment pe ON pe.to_id = e.id
            JOIN project p ON p.id = pe.from_id
            WHERE m2mc.to_id = :cid
            ORDER BY p.name, e.name, d.data_type
        """), {"cid": class_id}).mappings().all()

        stats_rows = conn.execute(text("""
            SELECT DISTINCT
                p.id AS proj_id, p.name AS proj_name,
                e.id AS exp_id, e.name AS exp_name,
                sr.id AS sr_id, sr.name AS sr_name, sr.data_type
            FROM metabolite_to_metabolite_class m2mc
            JOIN metabolite met ON met.id = m2mc.from_id
            JOIN measured_metabolite_to_metabolite mm2m ON mm2m.to_id = met.id
            JOIN measured_metabolite_stats_result__data mmsr ON mmsr.measured_metabolite_id = mm2m.from_id
            JOIN stats_result sr ON sr.id = mmsr.stats_result_id
            JOIN experiment_to_stats_result es ON es.to_id = sr.id
            JOIN experiment e ON e.id = es.from_id
            JOIN project_to_experiment pe ON pe.to_id = e.id
            JOIN project p ON p.id = pe.from_id
            WHERE m2mc.to_id = :cid
            ORDER BY p.name, e.name, sr.name
        """), {"cid": class_id}).mappings().all()

    experiments_map: dict = {}
    for row in dataset_rows:
        exp_id = row["exp_id"]
        if exp_id not in experiments_map:
            experiments_map[exp_id] = {
                "exp_id": exp_id, "exp_name": row["exp_name"],
                "proj_id": row["proj_id"], "proj_name": row["proj_name"],
                "experiment_type": row["experiment_type"],
                "datasets": [], "stats_results": [],
            }
        experiments_map[exp_id]["datasets"].append({
            "dataset_id": row["dataset_id"],
            "data_type": row["data_type"],
            "row_count": row["row_count"],
        })
    for row in stats_rows:
        exp_id = row["exp_id"]
        if exp_id not in experiments_map:
            experiments_map[exp_id] = {
                "exp_id": exp_id, "exp_name": row["exp_name"],
                "proj_id": row["proj_id"], "proj_name": row["proj_name"],
                "experiment_type": None, "datasets": [], "stats_results": [],
            }
        experiments_map[exp_id]["stats_results"].append({
            "sr_id": row["sr_id"],
            "sr_name": row["sr_name"],
            "data_type": row["data_type"],
        })

    ctx = {
        "request": request, "mc": dict(mc),
        "coverage": [dict(r) for r in coverage],
        "experiments": list(experiments_map.values()),
        "db_name": DB_NAME,
    }
    return _templates.TemplateResponse("demo_metabolite_class_detail.html", ctx)


@router.get("/metabolite-class-dataset-preview", response_class=HTMLResponse)
async def demo_metabolite_class_dataset_preview(
    request: Request, dataset_id: str, class_id: str
):
    engine = _get_engine()
    with engine.connect() as conn:
        total_row_count = conn.execute(
            text("SELECT row_count FROM dataset WHERE id = :id"), {"id": dataset_id}
        ).scalar() or 0

        data_rows = conn.execute(text("""
            SELECT mmd.measured_metabolite_id, mmd.run_biosample_id, mmd.value,
                   met.id AS met_id, COALESCE(met.name, met.id) AS met_name
            FROM measured_metabolite_dataset__data mmd
            JOIN measured_metabolite_to_metabolite mm2m ON mm2m.from_id = mmd.measured_metabolite_id
            JOIN metabolite met ON met.id = mm2m.to_id
            JOIN metabolite_to_metabolite_class m2mc ON m2mc.from_id = met.id
            WHERE mmd.dataset_id = :dataset_id AND m2mc.to_id = :class_id
            ORDER BY met_name, mmd.run_biosample_id
        """), {"dataset_id": dataset_id, "class_id": class_id}).mappings().all()

        seen_cols: dict = {}
        for row in data_rows:
            rb_id = row["run_biosample_id"]
            if rb_id not in seen_cols:
                seen_cols[rb_id] = True
        rb_ids = list(seen_cols.keys())

        condition_groups, col_order, meta_panel_rows = _fetch_column_metadata(conn, rb_ids)

    matrix: dict = {}
    for row in data_rows:
        key = (row["met_id"], row["met_name"])
        if key not in matrix:
            matrix[key] = {}
        val = row["value"]
        if val is not None:
            try:
                matrix[key][row["run_biosample_id"]] = f"{float(val):.3g}"
            except (TypeError, ValueError):
                matrix[key][row["run_biosample_id"]] = str(val)
        else:
            matrix[key][row["run_biosample_id"]] = None

    rows = [(met_id, met_name, vals) for (met_id, met_name), vals in matrix.items()]

    return _templates.TemplateResponse("demo_dataset_preview.html", {
        "request": request,
        "condition_groups": condition_groups,
        "col_order": col_order,
        "rows": rows,
        "meta_panel_rows": meta_panel_rows,
        "dataset_id": dataset_id,
        "pathway_analyte_count": len(rows),
        "analyte_label": "Metabolite",
        "analyte_url_prefix": "/demo/metabolites/",
        "total_row_count": total_row_count,
    })


@router.get("/metabolite-class-stats-preview", response_class=HTMLResponse)
async def demo_metabolite_class_stats_preview(
    request: Request, stats_result_id: str, class_id: str
):
    engine = _get_engine()
    with engine.connect() as conn:
        total_row_count = conn.execute(
            text("SELECT row_count FROM stats_result WHERE id = :id"), {"id": stats_result_id}
        ).scalar() or 0

        col_rows = conn.execute(text("""
            SELECT column_name, stat_type, group1, group2, comparison, notes
            FROM stats_result__comparison_columns
            WHERE parent_id = :sr_id
            ORDER BY id
        """), {"sr_id": stats_result_id}).mappings().all()

        data_rows = conn.execute(text("""
            SELECT mmsr.measured_metabolite_id, mmsr.column_name, mmsr.value,
                   met.id AS met_id, COALESCE(met.name, met.id) AS met_name
            FROM measured_metabolite_stats_result__data mmsr
            JOIN measured_metabolite_to_metabolite mm2m ON mm2m.from_id = mmsr.measured_metabolite_id
            JOIN metabolite met ON met.id = mm2m.to_id
            JOIN metabolite_to_metabolite_class m2mc ON m2mc.from_id = met.id
            WHERE mmsr.stats_result_id = :sr_id AND m2mc.to_id = :class_id
            ORDER BY met_name, mmsr.column_name
        """), {"sr_id": stats_result_id, "class_id": class_id}).mappings().all()

    comparisons_map: dict = {}
    col_order = []
    for row in col_rows:
        comp = row["comparison"] or ""
        if comp not in comparisons_map:
            comparisons_map[comp] = {
                "comparison": comp,
                "group1": row["group1"] or "",
                "group2": row["group2"] or "",
                "columns": [],
            }
        comparisons_map[comp]["columns"].append({
            "column_name": row["column_name"],
            "stat_type": row["stat_type"] or "",
            "notes": row["notes"] or "",
        })
        col_order.append(row["column_name"])

    matrix: dict = {}
    for row in data_rows:
        key = (row["met_id"], row["met_name"])
        if key not in matrix:
            matrix[key] = {}
        val = row["value"]
        if val is not None:
            try:
                matrix[key][row["column_name"]] = f"{float(val):.3g}"
            except (TypeError, ValueError):
                matrix[key][row["column_name"]] = str(val)
        else:
            matrix[key][row["column_name"]] = None

    rows = [(met_id, met_name, vals) for (met_id, met_name), vals in matrix.items()]

    return _templates.TemplateResponse("demo_stats_preview.html", {
        "request": request,
        "comparisons": list(comparisons_map.values()),
        "col_order": col_order,
        "rows": rows,
        "stats_result_id": stats_result_id,
        "pathway_analyte_count": len(rows),
        "analyte_label": "Metabolite",
        "analyte_url_prefix": "/demo/metabolites/",
        "total_row_count": total_row_count,
    })