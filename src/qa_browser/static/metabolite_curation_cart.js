(function () {
    const container = document.getElementById("metaboliteCurationCart");
    if (!container) return;

    const rootPath = container.dataset.rootPath || "";
    const curatorInput = container.querySelector("[data-curation-curator]");
    const counts = container.querySelectorAll("[data-curation-cart-count]");
    const status = container.querySelector("[data-curation-cart-status]");
    const items = container.querySelector("[data-curation-cart-items]");
    const toggle = container.querySelector("[data-curation-cart-toggle]");
    const close = container.querySelector("[data-curation-cart-close]");
    const publish = container.querySelector("[data-curation-publish]");
    const batchName = container.querySelector("[data-curation-batch-name]");
    const batchDescription = container.querySelector("[data-curation-batch-description]");
    const assertionName = container.querySelector("[data-assertion-name]");
    const assertionMemberIds = container.querySelector("[data-assertion-member-ids]");
    const assertionRationale = container.querySelector("[data-assertion-rationale]");
    const assertionAdd = container.querySelector("[data-assertion-add]");
    const identityStorageKey = "odinCurationCurator";
    const legacyIdentityStorageKey = "metaboliteHarmonizationCurator";
    const noticeStorageKey = "metaboliteHarmonizationCurationNotice";
    const startupNotice = sessionStorage.getItem(noticeStorageKey) || "";
    let cart = {operations: [], operation_count: 0};
    let loadTimer = null;

    function escapeHtml(value) {
        return String(value ?? "")
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;");
    }

    async function api(path, options = {}) {
        const response = await fetch(`${rootPath}${path}`, {
            ...options,
            headers: {"Content-Type": "application/json", ...(options.headers || {})},
        });
        let payload;
        try {
            payload = await response.json();
        } catch (_error) {
            payload = {};
        }
        if (!response.ok) {
            throw new Error(payload.detail || `Curation cart request failed (${response.status})`);
        }
        return payload;
    }

    function identityPayload() {
        const curator = curatorInput.value.trim();
        return {curator, curator_name: curator};
    }

    function defaultBatchName() {
        if (batchName.value.trim()) return;
        const curator = curatorInput.value.trim();
        if (!curator) return;
        batchName.value = `${curator}'s batch ${new Date().toISOString().slice(0, 10)}`;
    }

    function renderCart() {
        const operations = cart.operations || [];
        const decisionCount = operations.reduce((count, operation) => count + (
            operation.action === "set_properties"
                ? (operation.decisions || []).length
                : 1
        ), 0);
        counts.forEach((count) => {
            count.textContent = String(decisionCount);
        });
        publish.disabled = operations.length === 0;
        if (!operations.length) {
            items.innerHTML = '<p class="metabolite-curation-cart-empty">No pending changes. Add an annotation, edge decision, or validation assertion.</p>';
            return;
        }
        items.innerHTML = operations.map((operation) => `
            <article class="metabolite-curation-cart-item" data-curation-type="${escapeHtml(operation.curation_type || "")}">
                <div>
                    <small>${escapeHtml((operation.curation_type || "curation").replaceAll("_", " "))}</small>
                    ${Array.isArray(operation.decision_rows) ? `
                        <strong>Correct record properties</strong>
                        <span><code>${escapeHtml(operation.target_label || operation.target?.id || "")}</code></span>
                        <small>Graph: <code>${escapeHtml(operation.target?.curation_set || "")}</code></small>
                        <ul class="metabolite-curation-property-list">
                            ${(operation.decision_rows || []).map((decision) =>
                                `<li>${escapeHtml(decision.path_label || "property")}: <strong>${decision.mode === "remove_override" ? escapeHtml(decision.restore_label) : escapeHtml(decision.value_label)}</strong></li>`
                            ).join("")}
                        </ul>
                        ${operation.note ? `<small>${escapeHtml(operation.note)}</small>` : ""}
                    ` : ["suppress_record", "restore_record"].includes(operation.action) ? `
                        <strong>${operation.action === "suppress_record" ? "Suppress from harmonization" : "Restore to harmonization"}</strong>
                        <span><code>${escapeHtml(operation.target?.id || "")}</code></span>
                        ${operation.note ? `<small>${escapeHtml(operation.note)}</small>` : ""}
                    ` : operation.action === "assert_same_clique" ? `
                        <strong>Expected same clique: ${escapeHtml(operation.name)}</strong>
                        <span class="metabolite-curation-member-ids">${(operation.member_ids || []).map((id) => `<code>${escapeHtml(id)}</code>`).join(" ")}</span>
                        ${operation.rationale ? `<small>${escapeHtml(operation.rationale)}</small>` : ""}
                        ${(operation.missing_member_ids_at_add_time || []).length ? `<small class="metabolite-assertion-warning">Not currently found: ${operation.missing_member_ids_at_add_time.map(escapeHtml).join(", ")}</small>` : ""}
                    ` : operation.action === "retire_assertion" ? `
                        <strong>Retire expected-clique assertion</strong>
                        <span><code>${escapeHtml(operation.assertion_id)}</code></span>
                        ${operation.note ? `<small>${escapeHtml(operation.note)}</small>` : ""}
                    ` : ["accept_mw_discrepancy", "reopen_mw_discrepancy"].includes(operation.action) ? `
                        <strong>${operation.action === "accept_mw_discrepancy" ? "Accept MW discrepancy" : "Reopen MW discrepancy"}</strong>
                        <span><code>${escapeHtml(operation.target?.finding_id || "")}</code></span>
                        ${operation.reason ? `<small>${escapeHtml(operation.reason.replaceAll("_", " "))}</small>` : ""}
                        ${operation.note ? `<small>${escapeHtml(operation.note)}</small>` : ""}
                    ` : `
                        <strong>${operation.action === "retain_edge" ? "Retain equivalence edge" : "Remove equivalence edge"}</strong>
                        <span><code>${escapeHtml(operation.start_id)}</code> ↔ <code>${escapeHtml(operation.end_id)}</code></span>
                        ${operation.note ? `<small>${escapeHtml(operation.note)}</small>` : ""}
                    `}
                </div>
                <button type="button" class="btn" data-curation-remove-id="${escapeHtml(operation.operation_id)}" data-curation-remove-type="${escapeHtml(operation.curation_type || "")}">Remove</button>
            </article>
        `).join("");
    }

    function announceCartChanged() {
        const operations = (cart.operations || []).map((operation) => ({...operation}));
        window.metaboliteCurationCartOperations = operations;
        document.dispatchEvent(new CustomEvent("metabolite-curation-cart:changed", {
            detail: {operations},
        }));
    }

    function openCart() {
        if (window.closeFeedbackDrawer) window.closeFeedbackDrawer();
        container.classList.add("open");
        toggle.setAttribute("aria-expanded", "true");
    }

    function closeCart() {
        container.classList.remove("open");
        toggle.setAttribute("aria-expanded", "false");
    }

    function acceptCart(nextCart) {
        cart = nextCart || {operations: [], operation_count: 0};
        const returnedIdentity = cart.curator?.name || cart.curator?.id;
        if (!curatorInput.value.trim() && returnedIdentity) {
            curatorInput.value = returnedIdentity;
            localStorage.setItem(identityStorageKey, returnedIdentity);
        }
        defaultBatchName();
        renderCart();
        announceCartChanged();
    }

    async function loadCart(allowProxyIdentity = false) {
        const curator = curatorInput.value.trim();
        if (!curator && !allowProxyIdentity) {
            acceptCart({operations: [], operation_count: 0});
            status.textContent = "Enter your curator identity to load your saved cart.";
            return;
        }
        try {
            const params = new URLSearchParams(identityPayload());
            acceptCart(await api(`/api/curation-cart?${params}`));
            status.textContent = startupNotice || (cart.operations?.length
                ? "Draft autosaved in S3. It will survive a browser refresh."
                : "Cart loaded. New curations will be autosaved in S3.");
            if (startupNotice) sessionStorage.removeItem(noticeStorageKey);
        } catch (error) {
            status.textContent = error.message;
        }
    }

    async function addEdgeDecision(action, startId, endId, button) {
        openCart();
        if (!curatorInput.value.trim()) {
            status.textContent = "Enter your curator name or email before adding this edge removal.";
            curatorInput.focus();
            return;
        }
        if (!startId || !endId) {
            status.textContent = "This edge is missing an endpoint and cannot be added to the cart.";
            return;
        }
        const retaining = action === "retain_edge";
        status.textContent = `Saving ${retaining ? "retain" : "remove"} decision to your cart…`;
        if (button) button.disabled = true;
        try {
            acceptCart(await api("/ramp-id-qa/api/curation-cart/items", {
                method: "POST",
                body: JSON.stringify({...identityPayload(), action, start_id: startId, end_id: endId}),
            }));
            status.textContent = "Added and autosaved. This draft is not active until you publish it.";
        } catch (error) {
            status.textContent = error.message;
        } finally {
            if (button) button.disabled = false;
        }
    }

    async function addExpectedCliqueAssertion() {
        openCart();
        if (!curatorInput.value.trim()) {
            status.textContent = "Enter your curator name or email before adding an assertion.";
            curatorInput.focus();
            return;
        }
        status.textContent = "Saving expected-clique assertion to your cart…";
        assertionAdd.disabled = true;
        try {
            acceptCart(await api("/ramp-id-qa/api/curation-cart/items", {
                method: "POST",
                body: JSON.stringify({
                    ...identityPayload(),
                    action: "assert_same_clique",
                    name: assertionName.value.trim(),
                    member_ids: assertionMemberIds.value,
                    rationale: assertionRationale.value.trim(),
                }),
            }));
            assertionName.value = "";
            assertionMemberIds.value = "";
            assertionRationale.value = "";
            status.textContent = "Assertion added and autosaved. It will be evaluated after publication.";
        } catch (error) {
            status.textContent = error.message;
        } finally {
            assertionAdd.disabled = false;
        }
    }

    async function addPropertyDecisions(identifier, values, removeOverrides, note, button) {
        const pending = (cart.operations || []).find((operation) =>
            operation.curation_type === "metabolite_record_properties"
            && operation.action === "set_properties"
            && operation.target?.id === identifier
        );
        if (!Object.keys(values || {}).length && !(removeOverrides || []).length) {
            if (!pending) {
                return;
            }
            openCart();
            if (!curatorInput.value.trim()) {
                status.textContent = "Enter your curator name or email before changing this annotation.";
                curatorInput.focus();
                return;
            }
            status.textContent = "Removing the pending annotation from your review changes…";
            if (button) button.disabled = true;
            try {
                acceptCart(await api(`/ramp-id-qa/api/curation-cart/items/${encodeURIComponent(pending.operation_id)}`, {
                    method: "DELETE",
                    body: JSON.stringify({...identityPayload(), curation_type: pending.curation_type}),
                }));
                status.textContent = "Pending annotation removed; the published state is unchanged.";
            } catch (error) {
                status.textContent = error.message;
            } finally {
                if (button) button.disabled = false;
            }
            return;
        }
        openCart();
        if (!curatorInput.value.trim()) {
            status.textContent = "Enter your curator name or email before adding this annotation.";
            curatorInput.focus();
            return;
        }
        status.textContent = "Saving property changes to your review changes…";
        if (button) button.disabled = true;
        try {
            acceptCart(await api("/ramp-id-qa/api/curation-cart/items", {
                method: "POST",
                body: JSON.stringify({
                    ...identityPayload(), action: "set_properties", target_id: identifier,
                    values: values || {}, remove_overrides: removeOverrides || [],
                    note: note || "",
                }),
            }));
            status.textContent = "Added and autosaved. This decision is pending until publication.";
        } catch (error) {
            status.textContent = error.message;
        } finally {
            if (button) button.disabled = false;
        }
    }

    window.addMetabolitePropertyCurations = addPropertyDecisions;

    async function addRecordParticipationDecision(action, identifier, note, button) {
        openCart();
        if (!curatorInput.value.trim()) {
            status.textContent = "Enter your curator name or email before changing record participation.";
            curatorInput.focus();
            return;
        }
        if (!identifier) {
            status.textContent = "This record is missing an identifier.";
            return;
        }
        status.textContent = action === "suppress_record"
            ? "Adding record suppression to your review changes…"
            : "Adding record restoration to your review changes…";
        if (button) button.disabled = true;
        try {
            acceptCart(await api("/ramp-id-qa/api/curation-cart/items", {
                method: "POST",
                body: JSON.stringify({
                    ...identityPayload(),
                    action,
                    target_id: identifier,
                    note: note || "",
                    replace_target: true,
                }),
            }));
            status.textContent = "Added and autosaved. This decision is pending until publication.";
        } catch (error) {
            status.textContent = error.message;
        } finally {
            if (button) button.disabled = false;
        }
    }

    async function addAssertionRetirement(assertionId, button) {
        openCart();
        if (!curatorInput.value.trim()) {
            status.textContent = "Enter your curator name or email before retiring an assertion.";
            curatorInput.focus();
            return;
        }
        status.textContent = "Adding assertion retirement to your cart…";
        if (button) button.disabled = true;
        try {
            acceptCart(await api("/ramp-id-qa/api/curation-cart/items", {
                method: "POST",
                body: JSON.stringify({...identityPayload(), action: "retire_assertion", assertion_id: assertionId}),
            }));
            status.textContent = "Retirement added and autosaved. The assertion remains active until publication.";
        } catch (error) {
            status.textContent = error.message;
        } finally {
            if (button) button.disabled = false;
        }
    }

    async function addMwAdjudication(form) {
        openCart();
        if (!curatorInput.value.trim()) {
            status.textContent = "Enter your curator name or email before reviewing this finding.";
            curatorInput.focus();
            return;
        }
        const action = form.dataset.action;
        const note = form.querySelector("[data-mw-adjudication-note]")?.value.trim() || "";
        const reason = form.querySelector("[data-mw-adjudication-reason]")?.value || "";
        if (action === "accept_mw_discrepancy" && !reason) {
            status.textContent = "Choose why this MW discrepancy is acceptable.";
            return;
        }
        if (!note) {
            status.textContent = "Enter an explanatory note for this MW review.";
            form.querySelector("[data-mw-adjudication-note]")?.focus();
            return;
        }
        const button = form.querySelector('button[type="submit"]');
        status.textContent = "Saving MW validation decision to your cart…";
        if (button) button.disabled = true;
        try {
            acceptCart(await api("/ramp-id-qa/api/curation-cart/items", {
                method: "POST",
                body: JSON.stringify({
                    ...identityPayload(),
                    action,
                    stage_key: form.dataset.stageKey,
                    finding_id: form.dataset.findingId,
                    reason,
                    supporting_ids: form.querySelector("[data-mw-adjudication-supporting-ids]")?.value || "",
                    note,
                    replace_target: true,
                }),
            }));
            status.textContent = "MW validation decision added and autosaved. It becomes active after publication.";
        } catch (error) {
            status.textContent = error.message;
        } finally {
            if (button) button.disabled = false;
        }
    }

    const savedIdentity = localStorage.getItem(identityStorageKey)
        || localStorage.getItem(legacyIdentityStorageKey) || "";
    curatorInput.value = savedIdentity;
    if (savedIdentity && !localStorage.getItem(identityStorageKey)) {
        localStorage.setItem(identityStorageKey, savedIdentity);
    }
    curatorInput.addEventListener("input", () => {
        const curator = curatorInput.value.trim();
        if (curator) localStorage.setItem(identityStorageKey, curator);
        else localStorage.removeItem(identityStorageKey);
        clearTimeout(loadTimer);
        loadTimer = setTimeout(() => loadCart(false), 450);
    });

    toggle.addEventListener("click", () => {
        if (container.classList.contains("open")) closeCart();
        else openCart();
    });
    close.addEventListener("click", closeCart);
    assertionAdd.addEventListener("click", addExpectedCliqueAssertion);
    window.closeMetaboliteCurationCart = closeCart;

    document.addEventListener("click", (event) => {
        const addButton = event.target.closest(".metabolite-curation-add");
        if (addButton) {
            event.preventDefault();
            addEdgeDecision(
                addButton.dataset.curationAction || "remove_edge",
                addButton.dataset.curationStartId,
                addButton.dataset.curationEndId,
                addButton,
            );
            return;
        }
        const retireButton = event.target.closest("[data-assertion-retire-add]");
        if (retireButton) {
            event.preventDefault();
            addAssertionRetirement(retireButton.dataset.assertionRetireAdd, retireButton);
            return;
        }
        const removeButton = event.target.closest("[data-curation-remove-id]");
        if (!removeButton) return;
        status.textContent = "Removing curation from your cart…";
        api(`/api/curation-cart/items/${encodeURIComponent(removeButton.dataset.curationRemoveId)}`, {
            method: "DELETE",
            body: JSON.stringify({...identityPayload(), curation_type: removeButton.dataset.curationRemoveType}),
        }).then((nextCart) => {
            acceptCart(nextCart);
            status.textContent = "Removed and autosaved.";
        }).catch((error) => {
            status.textContent = error.message;
        });
    });

    document.addEventListener("submit", (event) => {
        const mwForm = event.target.closest("[data-mw-adjudication-form]");
        if (mwForm) {
            event.preventDefault();
            addMwAdjudication(mwForm);
            return;
        }
        const participationForm = event.target.closest("[data-metabolite-record-participation-form]");
        if (participationForm) {
            event.preventDefault();
            const action = participationForm.dataset.curationAction;
            const note = participationForm.querySelector("[data-record-participation-note]")?.value.trim() || "";
            if (action === "suppress_record" && !note) {
                status.textContent = "Enter a rationale before suppressing this record.";
                participationForm.querySelector("[data-record-participation-note]")?.focus();
                return;
            }
            addRecordParticipationDecision(
                action,
                participationForm.dataset.curationTargetId,
                note,
                participationForm.querySelector('button[type="submit"]'),
            );
            return;
        }
        const form = event.target.closest("[data-generic-classification-form]");
        if (!form) return;
        event.preventDefault();
        const choice = event.submitter?.dataset.genericClassificationChoice
            || form.querySelector("[data-generic-classification-value]:checked")?.value
            || "";
        const note = form.querySelector("[data-generic-classification-note]")?.value.trim() || "";
        if (!choice) {
            status.textContent = "Choose a generic-structure classification.";
            return;
        }
        if (!note) {
            status.textContent = "Enter a rationale for the classification change.";
            form.querySelector("[data-generic-classification-note]")?.focus();
            return;
        }
        const submitButton = form.querySelector('button[type="submit"]');
        addPropertyDecisions(
            form.dataset.curationTargetId,
            choice === "detected" ? {} : {
                is_generic_structure: choice === "null" ? null : choice === "true",
            },
            choice === "detected" ? ["is_generic_structure"] : [],
            note,
            submitButton,
        );
    });

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && container.classList.contains("open")) closeCart();
    });

    publish.addEventListener("click", async () => {
        const publishedOperations = [...(cart.operations || [])];
        status.textContent = "Publishing immutable curation batch…";
        publish.disabled = true;
        try {
            const result = await api("/api/curation-cart/publish", {
                method: "POST",
                body: JSON.stringify({
                    ...identityPayload(),
                    batch_name: batchName.value.trim(),
                    description: batchDescription.value.trim(),
                }),
            });
            if (result.partial) {
                const publishedTypes = (result.published || []).map((item) => item.curation_type.replaceAll("_", " ")).join(", ");
                const remainingTypes = (result.remaining_types || [result.failed_type || "remaining type"])
                    .map((item) => String(item).replaceAll("_", " ")).join(", ");
                const message = `Partially published ${publishedTypes}. Still pending: ${remainingTypes}. ${result.error}. Review and retry the remaining changes.`;
                sessionStorage.setItem(noticeStorageKey, message);
                document.dispatchEvent(new CustomEvent("metabolite-curations:published", {detail: result}));
                window.location.reload();
                return;
            }
            const identity = identityPayload();
            acceptCart({
                curator: {id: identity.curator, name: identity.curator_name},
                operations: [],
                operation_count: 0,
            });
            batchName.value = "";
            batchDescription.value = "";
            defaultBatchName();
            const hasGraphChanges = publishedOperations.some((operation) => [
                "remove_edge", "retain_edge", "set_properties",
                "suppress_record", "restore_record",
            ].includes(operation.action));
            const hasMwAdjudications = publishedOperations.some((operation) => [
                "accept_mw_discrepancy", "reopen_mw_discrepancy",
            ].includes(operation.action));
            status.textContent = hasGraphChanges
                ? `Published ${result.operation_count} item${result.operation_count === 1 ? "" : "s"} in ${result.batch_count} typed batch${result.batch_count === 1 ? "" : "es"}. Sync affected pipelines to apply them.`
                : hasMwAdjudications
                    ? `Published ${result.operation_count} validation decision${result.operation_count === 1 ? "" : "s"}. MW validation views now use the new review status.`
                    : `Published ${result.operation_count} assertion item${result.operation_count === 1 ? "" : "s"}. Validation views now use the new assertion set.`;
            sessionStorage.setItem(noticeStorageKey, status.textContent);
            document.dispatchEvent(new CustomEvent("metabolite-curations:published", {detail: result}));
            setTimeout(() => window.location.reload(), 900);
        } catch (error) {
            status.textContent = error.message;
            renderCart();
        }
    });

    renderCart();
    announceCartChanged();
    loadCart(true);
})();
