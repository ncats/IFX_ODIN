(function () {
    const buttons = Array.from(document.querySelectorAll("[data-registry-filter]"));
    if (!buttons.length) return;

    function setDetailRowVisible(row, visible) {
        if (row.nextElementSibling && row.nextElementSibling.classList.contains("registry-detail-row")) {
            row.nextElementSibling.hidden = !visible;
        }
    }

    function rowMatchesGraph(row, graphName) {
        if (graphName === "all") return true;
        const graphs = (row.dataset.graphs || "").split(/\s+/).filter(Boolean);
        return graphs.includes(graphName);
    }

    function setFilter(graphName) {
        buttons.forEach(function (button) {
            button.classList.toggle("active", button.dataset.registryFilter === graphName);
        });

        document.querySelectorAll("[data-registry-filter-group]").forEach(function (group) {
            let groupHasVisibleRows = false;
            const subgroups = group.querySelectorAll("[data-registry-filter-subgroup]");
            if (subgroups.length) {
                subgroups.forEach(function (subgroup) {
                    let subgroupHasVisibleRows = false;
                    subgroup.querySelectorAll("[data-registry-filter-row]").forEach(function (row) {
                        const visible = rowMatchesGraph(row, graphName);
                        row.hidden = !visible;
                        setDetailRowVisible(row, visible);
                        subgroupHasVisibleRows = subgroupHasVisibleRows || visible;
                    });
                    subgroup.hidden = !subgroupHasVisibleRows;
                    groupHasVisibleRows = groupHasVisibleRows || subgroupHasVisibleRows;
                });
            } else {
                group.querySelectorAll("[data-registry-filter-row]").forEach(function (row) {
                    const visible = rowMatchesGraph(row, graphName);
                    row.hidden = !visible;
                    setDetailRowVisible(row, visible);
                    groupHasVisibleRows = groupHasVisibleRows || visible;
                });
            }
            group.hidden = !groupHasVisibleRows;
        });
    }

    buttons.forEach(function (button) {
        button.addEventListener("click", function () {
            setFilter(button.dataset.registryFilter);
        });
    });
})();
