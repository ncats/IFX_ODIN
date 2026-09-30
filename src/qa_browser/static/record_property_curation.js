(function () {
    const card = document.getElementById("recordPropertyCuration");
    if (!card) return;

    const config = JSON.parse(card.dataset.recordCuration || "{}");
    const rootPath = card.dataset.rootPath || "";
    const curatorInput = document.getElementById("recordCurationCurator");
    const noteInput = document.getElementById("recordCurationNote");
    const status = document.getElementById("recordCurationStatus");
    const cart = document.getElementById("recordCurationCart");
    const cartBody = document.getElementById("recordCurationCartBody");
    const count = document.getElementById("recordCurationCartCount");
    const storageKey = "odinCurationCurator";
    const legacyStorageKey = "metaboliteHarmonizationCurator";

    function setEditorOpen(open) {
        const toggle = document.getElementById("recordCurationEditorToggle");
        if (!toggle || toggle.disabled) return;
        card.hidden = !open;
        toggle.setAttribute("aria-expanded", String(open));
        toggle.textContent = open ? "Close editor" : "Curate record";
        if (open) {
            card.scrollIntoView({behavior: "smooth", block: "start"});
            window.setTimeout(() => noteInput?.focus(), 200);
        }
    }

    function escapeHtml(value) {
        return String(value == null ? "" : value).replace(/[&<>'"]/g, ch => ({
            "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;"
        })[ch]);
    }

    function curator() {
        const value = (curatorInput && curatorInput.value || "").trim();
        if (!value) throw new Error("Enter your curator name first.");
        localStorage.setItem(storageKey, value);
        return value;
    }

    function parseInput(row) {
        const input = row.querySelector("[data-curation-input]");
        const kind = row.dataset.editorType;
        if (row.querySelector("[data-curation-null]")?.checked) return null;
        if (kind === "json" || kind === "bool") return JSON.parse(input.value);
        if (kind === "int") return input.value === "" ? null : Number.parseInt(input.value, 10);
        if (kind === "float") return input.value === "" ? null : Number.parseFloat(input.value);
        return input.value;
    }

    function sameValue(left, right) {
        return JSON.stringify(left) === JSON.stringify(right);
    }

    async function request(url, options) {
        const response = await fetch(`${rootPath}${url}`, options);
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(payload.detail || `Request failed (${response.status})`);
        return payload;
    }

    function renderCart(payload) {
        const operations = payload.operations || [];
        const decisionCount = operations.reduce((total, operation) => total + (
            Array.isArray(operation.decision_rows)
                ? (operation.decision_rows || operation.decisions || []).length
                : 1
        ), 0);
        count.textContent = String(decisionCount);
        if (!operations.length) {
            cartBody.innerHTML = "<p>No changes are waiting for publication.</p>";
            return;
        }
        cartBody.innerHTML = operations.map(operation => `
            <article class="record-curation-cart-item">
                <small>${escapeHtml((operation.curation_type || "curation").replaceAll("_", " "))}</small>
                ${Array.isArray(operation.decision_rows) ? `
                    <strong>${escapeHtml(operation.target_label || operation.target?.id || "")}</strong>
                    <small>Graph: <code>${escapeHtml(operation.target?.curation_set || "")}</code></small>
                    <ul>${(operation.decision_rows || []).map(decision => `
                        <li><strong>${escapeHtml(decision.path_label)}</strong>: ${decision.mode === "remove_override"
                            ? escapeHtml(decision.restore_label)
                            : escapeHtml(decision.value_label)}</li>`).join("")}</ul>
                ` : ["suppress_record", "restore_record"].includes(operation.action) ? `
                    <strong>${operation.action === "suppress_record" ? "Suppress record" : "Restore record"}</strong>
                    <span><code>${escapeHtml(operation.target?.id || "")}</code></span>
                ` : operation.action === "assert_same_clique" ? `
                    <strong>Expected same clique: ${escapeHtml(operation.name || "")}</strong>
                    <span>${(operation.member_ids || []).map(id => `<code>${escapeHtml(id)}</code>`).join(" ")}</span>
                ` : operation.action === "retire_assertion" ? `
                    <strong>Retire expected-clique assertion</strong>
                    <span><code>${escapeHtml(operation.assertion_id || "")}</code></span>
                ` : `
                    <strong>${operation.action === "retain_edge" ? "Retain equivalence edge" : "Remove equivalence edge"}</strong>
                    <span><code>${escapeHtml(operation.start_id || "")}</code> ↔ <code>${escapeHtml(operation.end_id || "")}</code></span>
                `}
                <p>${escapeHtml(operation.note || "")}</p>
                <button type="button" class="btn btn-secondary btn-sm" data-remove-operation="${escapeHtml(operation.operation_id)}" data-curation-type="${escapeHtml(operation.curation_type || "")}">Remove</button>
            </article>`).join("");
        cartBody.querySelectorAll("[data-remove-operation]").forEach(button => {
            button.addEventListener("click", async () => {
                try {
                    const payload = await request(`/api/curation-cart/items/${encodeURIComponent(button.dataset.removeOperation)}`, {
                        method: "DELETE",
                        headers: {"Content-Type": "application/json"},
                        body: JSON.stringify({curator: curator(), curator_name: curator(), curation_type: button.dataset.curationType}),
                    });
                    renderCart(payload);
                } catch (error) { status.textContent = error.message; }
            });
        });
    }

    async function loadCart() {
        const name = curator();
        renderCart(await request(`/api/curation-cart?curator=${encodeURIComponent(name)}&curator_name=${encodeURIComponent(name)}`));
    }

    if (curatorInput) {
        const savedIdentity = localStorage.getItem(storageKey)
            || localStorage.getItem(legacyStorageKey) || "";
        curatorInput.value = savedIdentity;
        if (savedIdentity && !localStorage.getItem(storageKey)) {
            localStorage.setItem(storageKey, savedIdentity);
        }
        curatorInput.addEventListener("change", () => loadCart().catch(error => { status.textContent = error.message; }));
        if (curatorInput.value) loadCart().catch(error => { status.textContent = error.message; });
    }

    document.getElementById("recordCurationStage")?.addEventListener("click", async () => {
        try {
            const note = (noteInput.value || "").trim();
            if (!note) throw new Error("Explain why the source values should change.");
            const decisions = [];
            card.querySelectorAll(".record-curation-field").forEach(row => {
                const value = parseInput(row);
                const effective = JSON.parse(row.dataset.effective);
                if (!sameValue(value, effective)) {
                    decisions.push({path: JSON.parse(row.dataset.curationPath), mode: "set", value});
                }
            });
            if (!decisions.length) throw new Error("No fields have changed.");
            const name = curator();
            const payload = await request("/api/record-curation-cart/items", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({
                    curator: name, curator_name: name,
                    database_name: config.database_name,
                    curation_set: config.curation_set,
                    model_type: config.model_type,
                    doc_key: config.doc_key,
                    decisions, note,
                }),
            });
            renderCart(payload);
            status.textContent = `${decisions.length} field change${decisions.length === 1 ? "" : "s"} added to review.`;
            cart.classList.add("open");
            cart.setAttribute("aria-hidden", "false");
        } catch (error) { status.textContent = error.message; }
    });

    card.querySelectorAll("[data-record-curation-restore]").forEach(button => {
        button.addEventListener("click", async () => {
            try {
                const note = (noteInput.value || "").trim();
                if (!note) throw new Error("Explain why this override should be restored.");
                const row = button.closest(".record-curation-field");
                const name = curator();
                const payload = await request("/api/record-curation-cart/items", {
                    method: "POST", headers: {"Content-Type": "application/json"},
                    body: JSON.stringify({
                        curator: name, curator_name: name,
                        database_name: config.database_name,
                        curation_set: config.curation_set, model_type: config.model_type,
                        doc_key: config.doc_key,
                        decisions: [{path: JSON.parse(row.dataset.curationPath), mode: "remove_override"}],
                        note,
                    }),
                });
                renderCart(payload);
                status.textContent = "Restoration added to review.";
            } catch (error) { status.textContent = error.message; }
        });
    });

    card.querySelectorAll("[data-curation-null]").forEach(checkbox => {
        const input = checkbox.closest(".record-curation-field").querySelector("[data-curation-input]");
        const sync = () => { input.disabled = checkbox.checked; };
        checkbox.addEventListener("change", sync);
        sync();
    });

    document.getElementById("recordCurationCartToggle")?.addEventListener("click", () => {
        cart.classList.add("open"); cart.setAttribute("aria-hidden", "false");
        loadCart().catch(error => { status.textContent = error.message; });
    });
    document.getElementById("recordCurationEditorToggle")?.addEventListener("click", () => {
        setEditorOpen(card.hidden);
    });
    document.getElementById("recordCurationEditorClose")?.addEventListener("click", () => {
        setEditorOpen(false);
    });
    document.getElementById("recordCurationCartClose")?.addEventListener("click", () => {
        cart.classList.remove("open"); cart.setAttribute("aria-hidden", "true");
    });
    document.getElementById("recordCurationPublish")?.addEventListener("click", async () => {
        try {
            const batchName = (document.getElementById("recordCurationBatchName").value || "").trim();
            if (!batchName) throw new Error("Enter a batch name.");
            const name = curator();
            const result = await request("/api/curation-cart/publish", {
                method: "POST", headers: {"Content-Type": "application/json"},
                body: JSON.stringify({
                    curator: name, curator_name: name, batch_name: batchName,
                    description: document.getElementById("recordCurationDescription").value || "",
                }),
            });
            if (result.partial) {
                renderCart(result.remaining_cart || {operations: []});
                const remaining = (result.remaining_types || []).join(", ");
                status.textContent = `Some curation types were published, but ${remaining || result.failed_type} remains pending: ${result.error}`;
                return;
            }
            status.textContent = `Published ${result.operation_count} curation${result.operation_count === 1 ? "" : "s"} in ${result.batch_count} typed batch${result.batch_count === 1 ? "" : "es"}.`;
            renderCart({operations: []});
            window.location.reload();
        } catch (error) { status.textContent = error.message; }
    });
})();
