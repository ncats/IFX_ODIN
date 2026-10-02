(function () {
    "use strict";

    const root = document.getElementById("metaboliteCurationReview");
    const dataElement = document.getElementById("metaboliteCurationRowsData");
    const form = document.getElementById("metaboliteCurationFilters");
    if (!root || !dataElement || !form) return;

    const allRows = JSON.parse(dataElement.textContent || "[]");
    const rootPath = root.dataset.rootPath || "";
    const resultCount = document.getElementById("metaboliteCurationResultCount");
    const tbody = document.getElementById("metaboliteCurationRows");
    const tableWrap = document.getElementById("metaboliteCurationTableWrap");
    const pagination = document.getElementById("metaboliteCurationPagination");
    const emptyState = document.getElementById("metaboliteCurationEmpty");
    const clearLink = document.getElementById("metaboliteCurationClear");
    const refreshButton = document.getElementById("metaboliteCurationRefresh");
    const applyButton = form.querySelector(".metabolite-curation-apply");
    const pageSize = 50;
    let page = Math.max(1, Number(new URLSearchParams(window.location.search).get("page")) || 1);
    let searchTimer = null;

    const facetDefinitions = {
        curation_type: {rowKey: "curation_type", labelKey: "type_label", many: false},
        source: {rowKey: "sources", many: true},
        batch: {rowKey: "batch_id", labelKey: "batch_name", many: false},
        curator: {rowKey: "curator", many: false},
    };

    if (applyButton) applyButton.style.display = "none";

    function selectedValues(name) {
        const select = form.elements[name];
        if (!select) return [];
        return Array.from(select.selectedOptions, option => option.value);
    }

    function filters() {
        return {
            q: String(form.elements.q.value || "").trim(),
            curation_type: selectedValues("curation_type"),
            source: selectedValues("source"),
            batch: selectedValues("batch"),
            curator: selectedValues("curator"),
        };
    }

    function rowMatches(row, state, excludedFacet) {
        for (const [facetName, definition] of Object.entries(facetDefinitions)) {
            if (facetName === excludedFacet || !state[facetName].length) continue;
            if (definition.many) {
                if (!state[facetName].some(value => (row[definition.rowKey] || []).includes(value))) return false;
            } else if (!state[facetName].includes(String(row[definition.rowKey] || ""))) {
                return false;
            }
        }
        if (state.q) {
            const haystack = [
                row.primary, row.secondary, row.note, row.batch_id,
                row.batch_name, row.curator, ...(row.sources || []),
            ].join(" ").toLocaleLowerCase();
            if (!haystack.includes(state.q.toLocaleLowerCase())) return false;
        }
        return true;
    }

    function contextualOptions(facetName, state) {
        const definition = facetDefinitions[facetName];
        const counts = new Map();
        const labels = new Map();
        allRows.filter(row => rowMatches(row, state, facetName)).forEach(row => {
            const values = definition.many ? (row[definition.rowKey] || []) : [row[definition.rowKey]];
            values.filter(Boolean).forEach(value => {
                const text = String(value);
                counts.set(text, (counts.get(text) || 0) + 1);
                if (definition.labelKey && row[definition.labelKey]) labels.set(text, row[definition.labelKey]);
            });
        });
        state[facetName].forEach(value => {
            if (!counts.has(value)) counts.set(value, 0);
            if (!labels.has(value)) {
                const row = allRows.find(item => String(item[definition.rowKey] || "") === value);
                if (row && definition.labelKey) labels.set(value, row[definition.labelKey]);
            }
        });
        return Array.from(counts, ([value, count]) => ({
            value,
            count,
            label: labels.get(value) || value,
        })).sort((left, right) => right.count - left.count || left.label.localeCompare(right.label));
    }

    function renderFacets(state) {
        for (const facetName of Object.keys(facetDefinitions)) {
            const select = form.elements[facetName];
            const selected = new Set(state[facetName]);
            select.replaceChildren(...contextualOptions(facetName, state).map(item => {
                const option = document.createElement("option");
                option.value = item.value;
                option.textContent = `${item.label} (${item.count.toLocaleString()})`;
                option.selected = selected.has(item.value);
                return option;
            }));
        }
    }

    function textElement(tag, value, className) {
        const element = document.createElement(tag);
        if (className) element.className = className;
        element.textContent = value == null ? "" : String(value);
        return element;
    }

    function appendLine(parent, label, value, options) {
        if (!value) return;
        const line = document.createElement("div");
        if (label) line.append(document.createTextNode(`${label}: `));
        if (options && options.code) line.append(textElement("code", value));
        else if (options && options.link) {
            const link = document.createElement("a");
            link.href = options.href || value;
            link.textContent = value;
            link.rel = "noopener noreferrer";
            line.append(link);
        } else line.append(document.createTextNode(String(value)));
        parent.append(line);
    }

    function formatDate(value) {
        if (!value) return "";
        const day = String(value).slice(0, 10);
        const date = new Date(`${day}T00:00:00Z`);
        if (Number.isNaN(date.getTime())) return day;
        return new Intl.DateTimeFormat(undefined, {
            year: "numeric", month: "short", day: "numeric", timeZone: "UTC",
        }).format(date);
    }

    function safeHttpUrl(value) {
        if (!value) return "";
        try {
            const url = new URL(String(value), window.location.href);
            return ["http:", "https:"].includes(url.protocol) ? url.href : "";
        } catch (_error) {
            return "";
        }
    }

    function renderDecision(row) {
        const cell = document.createElement("td");
        if (row.kind === "property") {
            const heading = document.createElement("div");
            heading.append(textElement("strong", row.property_path));
            cell.append(heading);
            const change = document.createElement("div");
            change.className = "metabolite-curation-value-change";
            change.append(textElement("code", row.observed_value), document.createTextNode(" → "), textElement("code", row.curated_value));
            cell.append(change);
        } else if (row.kind === "mw" && row.reason) {
            cell.append(textElement("div", row.reason.replaceAll("_", " ").replace(/\b\w/g, value => value.toUpperCase())));
        }
        cell.append(textElement("small", row.note));
        return cell;
    }

    function renderProvenance(row) {
        const cell = document.createElement("td");
        const name = document.createElement("div");
        name.append(textElement("strong", row.batch_name));
        const id = document.createElement("div");
        id.append(textElement("code", row.batch_id));
        cell.append(name, id);
        const dateLine = row.provenance_date
            ? `${row.curator} · ${row.provenance_date_label} ${formatDate(row.provenance_date)}`
            : row.curator;
        cell.append(textElement("div", dateLine));
        if (!row.origin) return cell;

        const details = document.createElement("details");
        details.className = "metabolite-curation-origin";
        details.append(textElement("summary", "Original source"));
        appendLine(details, "", row.origin.repository);
        if (row.origin.path) {
            const path = document.createElement("div");
            path.append(textElement("code", row.origin.path));
            details.append(path);
        }
        appendLine(details, "Commit", row.origin.commit ? row.origin.commit.slice(0, 12) : "", {code: true});
        if (row.origin.attributed_curator && row.origin.attributed_curator !== "Not recorded") {
            appendLine(details, "Attributed from Git", row.origin.attributed_curator);
        } else if (row.origin.raw_author && row.origin.raw_author !== "Not recorded") {
            appendLine(details, "Git author", row.origin.raw_author);
        }
        if (row.origin.attribution_evidence_label) {
            const evidenceUrl = safeHttpUrl(row.origin.attribution_evidence_url);
            appendLine(details, "Evidence", row.origin.attribution_evidence_label, {
                link: Boolean(evidenceUrl),
                href: evidenceUrl,
            });
        }
        appendLine(details, "Git commit authored", formatDate(row.origin.authored_at));
        if (row.origin.authored_at) appendLine(details, "Included in curation release", formatDate(row.published_at));
        cell.append(details);
        return cell;
    }

    function renderRow(row) {
        const tr = document.createElement("tr");
        tr.append(textElement("td", row.type_label));

        const subject = document.createElement("td");
        subject.append(textElement("code", row.primary));
        if (row.secondary) subject.append(textElement("div", row.secondary, "metabolite-curation-secondary"));
        tr.append(subject, renderDecision(row));

        const sources = document.createElement("td");
        const sourceList = document.createElement("div");
        sourceList.className = "metabolite-curation-source-list";
        (row.sources || []).forEach(source => sourceList.append(textElement("span", source)));
        sources.append(sourceList);
        tr.append(sources, renderProvenance(row));

        const review = document.createElement("td");
        const link = document.createElement("a");
        link.className = "btn btn-secondary btn-sm";
        link.textContent = "Inspect";
        link.href = rootPath + (row.review_path || `/ramp-id-qa?${row.review_query || ""}`);
        review.append(link);
        tr.append(review);
        return tr;
    }

    function syncUrl(state) {
        const params = new URLSearchParams();
        if (state.q) params.set("q", state.q);
        Object.keys(facetDefinitions).forEach(name => state[name].forEach(value => params.append(name, value)));
        if (page > 1) params.set("page", String(page));
        const query = params.toString();
        window.history.replaceState(null, "", `${window.location.pathname}${query ? `?${query}` : ""}`);
    }

    function render() {
        const state = filters();
        const filtered = allRows.filter(row => rowMatches(row, state));
        const pageCount = Math.max(1, Math.ceil(filtered.length / pageSize));
        page = Math.min(Math.max(page, 1), pageCount);
        const start = (page - 1) * pageSize;
        const visible = filtered.slice(start, start + pageSize);

        renderFacets(state);
        tbody.replaceChildren(...visible.map(renderRow));
        resultCount.textContent = `${filtered.length.toLocaleString()} matching of ${allRows.length.toLocaleString()} active decisions.`;
        tableWrap.hidden = visible.length === 0;
        emptyState.hidden = visible.length !== 0;
        pagination.hidden = visible.length === 0;
        pagination.replaceChildren();
        if (page > 1) {
            const previous = textElement("button", "Previous", "btn btn-secondary");
            previous.type = "button";
            previous.addEventListener("click", () => { page -= 1; render(); });
            pagination.append(previous);
        }
        pagination.append(textElement("span", `Page ${page} of ${pageCount}`));
        if (page < pageCount) {
            const next = textElement("button", "Next", "btn btn-secondary");
            next.type = "button";
            next.addEventListener("click", () => { page += 1; render(); });
            pagination.append(next);
        }
        syncUrl(state);
    }

    form.addEventListener("submit", event => {
        event.preventDefault();
        page = 1;
        render();
    });
    form.querySelectorAll("select").forEach(select => select.addEventListener("change", () => {
        page = 1;
        render();
    }));
    form.elements.q.addEventListener("input", () => {
        window.clearTimeout(searchTimer);
        searchTimer = window.setTimeout(() => { page = 1; render(); }, 150);
    });
    clearLink.addEventListener("click", event => {
        event.preventDefault();
        form.reset();
        Array.from(form.querySelectorAll("select option")).forEach(option => { option.selected = false; });
        form.elements.q.value = "";
        page = 1;
        render();
    });
    refreshButton.addEventListener("click", () => window.location.reload());
    window.addEventListener("popstate", () => window.location.reload());

    render();
})();
