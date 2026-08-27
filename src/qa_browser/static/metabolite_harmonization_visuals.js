(function (global) {
    "use strict";

    function withStructureImages(elements) {
        return (elements || []).map((element) => {
            if (!element.data || element.data.source || element.data.target) return element;
            if (String(element.classes || "").split(/\s+/).includes("has-structure")) return element;
            const smiles = Array.from(new Set(element.data.smiles || [])).filter(Boolean);
            const structure = smiles[0] || element.data.chemical_entity?.smiles || "";
            if (!structure) return element;
            return {
                ...element,
                classes: `${element.classes || ""} has-structure`.trim(),
                data: {
                    ...element.data,
                    structure_image: `https://opendata.ncats.nih.gov/renderer/render?structure=${encodeURIComponent(structure)}&size=220`,
                },
            };
        });
    }

    function escapeHtml(value) {
        return String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;").replaceAll('"', "&quot;");
    }

    function distinctValues(values) {
        return Array.from(new Set((values || []).map((value) => String(value ?? "").trim()).filter(Boolean)));
    }

    function formatInlineList(values, maxItems) {
        const distinct = distinctValues(values);
        if (!distinct.length) return "";
        const shown = distinct.slice(0, maxItems).map((value) => `<code>${escapeHtml(value)}</code>`).join(", ");
        return distinct.length > maxItems
            ? `${shown} <span class="subtle">+${distinct.length - maxItems} more</span>`
            : shown;
    }

    function detailRow(label, value, html) {
        if (value === undefined || value === null || value === "") return "";
        return `<div><strong>${escapeHtml(label)}</strong><span>${html ? value : escapeHtml(value)}</span></div>`;
    }

    function renderChemPropTable(records) {
        if (!records || !records.length) return "";
        const rows = records.map((prop) => `<tr>
            <td>${escapeHtml(prop.source || "")}</td><td><code>${escapeHtml(prop.source_id || "")}</code></td>
            <td>${escapeHtml(prop.formula || "")}</td><td>${escapeHtml(prop.mw || "")}</td>
            <td>${escapeHtml(prop.monoisotopic_mass || "")}</td>
        </tr>`).join("");
        return `<section class="metabolite-id-detail-section"><h4>Chemical Property Records</h4>
            <div class="metabolite-id-detail-table-wrap"><table class="ramp-id-table">
            <thead><tr><th>Source</th><th>Source ID</th><th>Formula</th><th>MW</th><th>Mono Mass</th></tr></thead>
            <tbody>${rows}</tbody></table></div></section>`;
    }

    function renderStructureData(data) {
        const smiles = distinctValues(data.smiles || []);
        const inchiKeys = distinctValues(data.inchi_keys || []);
        const entity = data.chemical_entity || {};
        const structure = smiles[0] || entity.smiles || "";
        const image = structure ? `<div class="metabolite-id-structure-render">
            <img src="https://opendata.ncats.nih.gov/renderer/render?structure=${encodeURIComponent(structure)}&size=320"
                 alt="Chemical structure rendering"></div>` : "";
        const rows = [
            detailRow("SMILES", formatInlineList(smiles, 3), true),
            detailRow("InChIKey", formatInlineList(inchiKeys, 4), true),
            detailRow("ChEBI SMILES", entity.smiles ? `<code>${escapeHtml(entity.smiles)}</code>` : "", true),
            detailRow("ChEBI InChIKey", entity.inchi_key ? `<code>${escapeHtml(entity.inchi_key)}</code>` : "", true),
        ].filter(Boolean).join("");
        if (!image && !rows) return "";
        return `<section class="metabolite-id-detail-section"><h4>Structure Data</h4>${image}<div class="ramp-id-details">${rows}</div></section>`;
    }

    function selectionDetails(data, isEdge) {
        if (isEdge) {
            const keys = ["kind", "source", "target", "label", "sources", "rule_label", "rule_id",
                "algorithm_version", "source_count", "detail_count", "snapshot", "snapshot_key"];
            return {
                title: data.label || data.id || "Selection",
                subtitle: `${data.start_id || data.source} → ${data.end_id || data.target}`,
                html: keys.map((key) => detailRow(key, data[key])).filter(Boolean).join(""),
            };
        }
        const entity = data.chemical_entity || {};
        const sourceLink = data.source_url
            ? `<a href="${escapeHtml(data.source_url)}" target="_blank" rel="noopener noreferrer">Open ${escapeHtml(data.source_label || "source record")}</a>`
            : "";
        const identifierRows = [
            detailRow("ID", `<code>${escapeHtml(data.id || "")}</code>`, true),
            detailRow("Source", sourceLink, true),
            detailRow("Prefix", data.prefix),
            detailRow("Names", formatInlineList(data.names || [], 6), true),
            detailRow("Name count", data.name_count),
            detailRow("Synonym count", data.synonym_count),
        ].filter(Boolean).join("");
        const chemistryRows = [
            detailRow("MW", data.mass_summary
                ? `min ${data.mass_summary.min} · median ${data.mass_summary.median} · max ${data.mass_summary.max} (${data.mass_summary.count})`
                : ""),
            detailRow("Formula", formatInlineList(data.formulas || [], 6), true),
            detailRow("Chem prop records", data.chem_prop_count),
            detailRow("ChEBI entity", entity.id
                ? `<code>${escapeHtml(entity.id)}</code>${entity.name ? ` · ${escapeHtml(entity.name)}` : ""}`
                : "", true),
            detailRow("ChEBI mass", entity.mass),
            detailRow("ChEBI mono mass", entity.monoisotopic_mass),
            detailRow("ChEBI formula", entity.formula ? `<code>${escapeHtml(entity.formula)}</code>` : "", true),
        ].filter(Boolean).join("");
        return {
            title: data.label || data.node_label || data.id || "Selection",
            subtitle: data.kind || data.prefix || "MetaboliteIdentifier",
            html: [
                `<section class="metabolite-id-detail-section"><h4>Identifier</h4><div class="ramp-id-details">${identifierRows}</div></section>`,
                `<section class="metabolite-id-detail-section"><h4>Chemistry</h4><div class="ramp-id-details">${chemistryRows}</div></section>`,
                renderStructureData(data),
                renderChemPropTable(data.chem_props || []),
            ].filter(Boolean).join(""),
        };
    }

    function cliqueStyles() {
        return [
            {selector: "node", style: {
                "label": "data(node_label)", "font-size": 9, "font-weight": 700,
                "text-halign": "center", "text-valign": "bottom", "text-margin-y": -5,
                "text-wrap": "wrap", "text-max-width": 62, "text-background-color": "#ffffff",
                "text-background-opacity": 0.88, "text-background-padding": 2,
                "background-color": "#64748b", "border-color": "#334155", "border-width": 1,
                "shape": "ellipse", "width": 82, "height": 82,
            }},
            {selector: ".prefix-chebi", style: {"background-color": "#2563eb"}},
            {selector: ".prefix-hmdb", style: {"background-color": "#059669"}},
            {selector: ".prefix-pubchem-compound", style: {"background-color": "#9333ea"}},
            {selector: ".prefix-kegg-compound", style: {"background-color": "#d97706"}},
            {selector: ".prefix-refmet", style: {"background-color": "#dc2626"}},
            {selector: ".has-structure", style: {
                "background-color": "#ffffff", "background-image": "data(structure_image)",
                "background-fit": "contain", "background-height": "76%", "background-width": "76%",
                "background-clip": "node", "background-image-opacity": 1,
                "background-image-crossorigin": "null",
            }},
            {selector: ".selected-query", style: {
                "border-color": "#f59e0b", "border-width": 5, "width": 82, "height": 82,
            }},
            {selector: "edge", style: {
                "curve-style": "bezier", "line-color": "#94a3b8", "target-arrow-color": "#94a3b8",
                "target-arrow-shape": "triangle", "width": 1.5, "label": "data(label)",
                "font-size": 8, "text-rotation": "autorotate",
            }},
            {selector: ".metabolite-equivalence-edge", style: {
                "line-style": "solid", "line-color": "#2563eb", "target-arrow-shape": "none",
                "target-arrow-color": "#2563eb", "width": 2.5, "opacity": 0.85,
            }},
            {selector: ".ifx-harmonization-rule-edge", style: {
                "line-color": "#e11d48", "target-arrow-color": "#e11d48", "width": 4,
                "opacity": 1, "color": "#9f1239", "font-weight": 700,
                "text-background-color": "#fff1f2", "text-background-opacity": 0.92,
                "text-background-padding": 2,
            }},
            {selector: ".pending-curation-removal", style: {
                "line-color": "#f59e0b", "target-arrow-color": "#f59e0b", "line-style": "dashed",
                "width": 6, "opacity": 1, "z-index": 20,
            }},
            {selector: ".denylist-review-node", style: {"border-color": "#dc2626", "border-width": 6}},
            {selector: ".denylist-review-edge", style: {
                "line-color": "#dc2626", "target-arrow-shape": "none", "line-style": "dashed",
                "width": 5, "opacity": 1, "label": "data(label)", "color": "#991b1b",
                "font-weight": 700, "text-background-color": "#ffffff",
                "text-background-opacity": 0.9, "text-background-padding": 2, "z-index": 30,
            }},
            {selector: ".denylist-review-edge.pending-curation-retain", style: {
                "line-color": "#16a34a", "color": "#166534", "width": 7,
                "label": "pending retention",
            }},
            {selector: ":selected", style: {"border-width": 5, "border-color": "#f59e0b"}},
        ];
    }

    function renderCliqueGraph(options) {
        if (!global.cytoscape || !options.container) return null;
        const cy = global.cytoscape({
            container: options.container,
            elements: withStructureImages(options.elements || []),
            layout: {name: "cose", animate: false, fit: true, padding: options.padding || 36},
            style: cliqueStyles(),
        });
        if (options.onNodeTap) cy.on("tap", "node", (event) => {
            event.target.select();
            options.onNodeTap(event.target.data(), event.target);
        });
        if (options.onEdgeTap) cy.on("tap", "edge", (event) => options.onEdgeTap(event.target.data(), event.target));
        cy.ready(() => {
            cy.fit(undefined, options.padding || 36);
            if (options.onReady) options.onReady(cy);
        });
        return cy;
    }

    function renderSankey(options) {
        const d3 = global.d3;
        const container = options.container;
        const payload = options.payload || {nodes: [], links: []};
        if (!container || !d3 || !d3.sankey) return;
        container.innerHTML = "";
        if (!payload.nodes.length) {
            container.innerHTML = '<div class="ramp-id-structure-empty">No changed clique flow is available.</div>';
            return;
        }
        const width = Math.max(options.minWidth || 720, container.clientWidth || 720);
        const height = Math.max(options.minHeight || 320, Math.min(900, 150 + payload.nodes.length * 34));
        const margin = {top: 24, right: 24, bottom: 24, left: 24};
        const graph = {
            nodes: payload.nodes.map((node) => ({...node})),
            links: payload.links.map((link) => ({...link})),
        };
        const stageKeys = Array.from(new Set(graph.nodes
            .slice().sort((a, b) => Number(a.stageIndex || 0) - Number(b.stageIndex || 0))
            .map((node) => node.snapshot_key)));
        const stageColor = d3.scaleOrdinal().domain(stageKeys).range(d3.schemeTableau10);
        const svg = d3.select(container).append("svg")
            .attr("width", width).attr("height", height).attr("viewBox", [0, 0, width, height]);
        const sankey = d3.sankey().nodeId((node) => node.id).nodeWidth(18).nodePadding(14)
            .nodeAlign((node) => node.stageIndex)
            .extent([[margin.left, margin.top], [width - margin.right, height - margin.bottom]]);
        sankey(graph);
        const showTip = (event, html) => {
            if (!options.tip) return;
            options.tip.innerHTML = html;
            options.tip.style.opacity = "1";
            const box = options.tip.getBoundingClientRect();
            let left = event.clientX + 14;
            let top = event.clientY + 14;
            if (left + box.width + 14 > global.innerWidth) left = event.clientX - box.width - 14;
            if (top + box.height + 14 > global.innerHeight) top = event.clientY - box.height - 14;
            options.tip.style.left = `${Math.max(14, left)}px`;
            options.tip.style.top = `${Math.max(14, top)}px`;
        };
        const hideTip = () => { if (options.tip) options.tip.style.opacity = "0"; };
        svg.append("g").attr("fill", "none").selectAll("path").data(graph.links).join("path")
            .attr("d", d3.sankeyLinkHorizontal())
            .attr("stroke", (link) => link.source.is_singleton_bucket || link.target.is_singleton_bucket
                ? "#94a3b8" : stageColor(link.source.snapshot_key))
            .attr("stroke-width", (link) => Math.max(1, link.width)).attr("stroke-opacity", 0.62)
            .on("mousemove", (event, link) => showTip(event, options.formatHover(link) || ""))
            .on("mouseleave", hideTip);
        const nodes = svg.append("g").selectAll("g").data(graph.nodes).join("g");
        nodes.append("rect")
            .attr("x", (node) => node.x0).attr("y", (node) => node.y0)
            .attr("height", (node) => Math.max(1, node.y1 - node.y0)).attr("width", (node) => node.x1 - node.x0)
            .attr("rx", 3).attr("fill", (node) => node.is_singleton_bucket ? "#94a3b8" : stageColor(node.snapshot_key))
            .attr("stroke", (node) => node.id === options.selectedNodeId ? "#f59e0b" : "rgba(15, 23, 42, 0.35)")
            .attr("stroke-width", (node) => node.id === options.selectedNodeId ? 3 : 1)
            .style("cursor", "pointer")
            .on("mousemove", (event, node) => showTip(event, options.formatHover(node) || ""))
            .on("mouseleave", hideTip)
            .on("click", (_event, node) => { if (options.onNodeClick) options.onNodeClick(node); });
        nodes.append("text")
            .attr("x", (node) => node.x0 < width / 2 ? node.x1 + 8 : node.x0 - 8)
            .attr("y", (node) => (node.y0 + node.y1) / 2 + 4)
            .attr("text-anchor", (node) => node.x0 < width / 2 ? "start" : "end")
            .attr("font-size", 11).attr("font-weight", 700)
            .text((node) => node.name)
            .each(function () {
                const text = d3.select(this);
                const parts = text.text().split("\n");
                text.text("");
                parts.forEach((part, index) => text.append("tspan").attr("x", text.attr("x"))
                    .attr("dy", index ? 13 : 0).text(part.length > 32 ? `${part.slice(0, 29)}...` : part));
            });
    }

    global.MetaboliteHarmonizationVisuals = {
        cliqueStyles,
        renderCliqueGraph,
        renderSankey,
        selectionDetails,
        withStructureImages,
    };
})(window);
