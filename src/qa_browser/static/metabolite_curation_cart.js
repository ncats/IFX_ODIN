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
    const identityStorageKey = "metaboliteHarmonizationCurator";
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
        counts.forEach((count) => {
            count.textContent = String(operations.length);
        });
        publish.disabled = operations.length === 0;
        if (!operations.length) {
            items.innerHTML = '<p class="metabolite-curation-cart-empty">Your cart is empty. Add an edge decision or expected-clique assertion.</p>';
            return;
        }
        items.innerHTML = operations.map((operation) => `
            <article class="metabolite-curation-cart-item">
                <div>
                    ${operation.action === "assert_same_clique" ? `
                        <strong>Expected same clique: ${escapeHtml(operation.name)}</strong>
                        <span class="metabolite-curation-member-ids">${(operation.member_ids || []).map((id) => `<code>${escapeHtml(id)}</code>`).join(" ")}</span>
                        ${operation.rationale ? `<small>${escapeHtml(operation.rationale)}</small>` : ""}
                        ${(operation.missing_member_ids_at_add_time || []).length ? `<small class="metabolite-assertion-warning">Not currently found: ${operation.missing_member_ids_at_add_time.map(escapeHtml).join(", ")}</small>` : ""}
                    ` : operation.action === "retire_assertion" ? `
                        <strong>Retire expected-clique assertion</strong>
                        <span><code>${escapeHtml(operation.assertion_id)}</code></span>
                        ${operation.note ? `<small>${escapeHtml(operation.note)}</small>` : ""}
                    ` : `
                        <strong>${operation.action === "retain_edge" ? "Retain equivalence edge" : "Remove equivalence edge"}</strong>
                        <span><code>${escapeHtml(operation.start_id)}</code> ↔ <code>${escapeHtml(operation.end_id)}</code></span>
                        ${operation.note ? `<small>${escapeHtml(operation.note)}</small>` : ""}
                    `}
                </div>
                <button type="button" class="btn" data-curation-remove-id="${escapeHtml(operation.operation_id)}">Remove</button>
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
            acceptCart(await api(`/ramp-id-qa/api/curation-cart?${params}`));
            status.textContent = cart.operations?.length
                ? "Draft autosaved in S3. It will survive a browser refresh."
                : "Cart loaded. New curations will be autosaved in S3.";
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

    curatorInput.value = localStorage.getItem(identityStorageKey) || "";
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
        api(`/ramp-id-qa/api/curation-cart/items/${encodeURIComponent(removeButton.dataset.curationRemoveId)}`, {
            method: "DELETE",
            body: JSON.stringify(identityPayload()),
        }).then((nextCart) => {
            acceptCart(nextCart);
            status.textContent = "Removed and autosaved.";
        }).catch((error) => {
            status.textContent = error.message;
        });
    });

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && container.classList.contains("open")) closeCart();
    });

    publish.addEventListener("click", async () => {
        const publishedOperations = [...(cart.operations || [])];
        status.textContent = "Publishing immutable curation batch…";
        publish.disabled = true;
        try {
            const result = await api("/ramp-id-qa/api/curation-cart/publish", {
                method: "POST",
                body: JSON.stringify({
                    ...identityPayload(),
                    batch_name: batchName.value.trim(),
                    description: batchDescription.value.trim(),
                }),
            });
            const identity = identityPayload();
            acceptCart({
                curator: {id: identity.curator, name: identity.curator_name},
                operations: [],
                operation_count: 0,
            });
            batchName.value = "";
            batchDescription.value = "";
            defaultBatchName();
            const hasGraphChanges = publishedOperations.some((operation) => ["remove_edge", "retain_edge"].includes(operation.action));
            status.textContent = hasGraphChanges
                ? `Published ${result.operation_count} item${result.operation_count === 1 ? "" : "s"}. Sync affected pipelines to apply edge decisions.`
                : `Published ${result.operation_count} assertion item${result.operation_count === 1 ? "" : "s"}. Validation views now use the new assertion set.`;
        } catch (error) {
            status.textContent = error.message;
            renderCart();
        }
    });

    renderCart();
    announceCartChanged();
    loadCart(true);
})();
