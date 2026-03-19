"use strict";

(function () {
    var REFRESH_INTERVAL_MS = 30000;

    var listState = {
        alerts: [],
        severityFilter: "ALL",
        knownAlertIds: new Set(),
        hasRendered: false,
        timer: null
    };

    var detailState = {
        alertId: "",
        timer: null
    };

    function fetchJson(url, options) {
        return fetch(url, options || {
            headers: {
                "Accept": "application/json"
            }
        }).then(function (response) {
            if (!response.ok) {
                throw new Error("Request failed for " + url + " (" + response.status + ")");
            }
            return response.json();
        });
    }

    function formatDateTime(value) {
        var date = new Date(value);
        if (isNaN(date.getTime())) {
            return value || "-";
        }
        return date.toLocaleString();
    }

    function formatScore(value) {
        var score = Number(value);
        if (!isFinite(score)) {
            return "-";
        }
        return score.toFixed(4);
    }

    function escapeHtml(text) {
        return String(text)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function truncateText(text, maxLength) {
        var raw = String(text || "");
        if (raw.length <= maxLength) {
            return raw;
        }
        return raw.slice(0, maxLength - 3) + "...";
    }

    function severityClass(severity) {
        if (severity === "CRITICAL") {
            return "severity-critical";
        }
        if (severity === "WARNING") {
            return "severity-warning";
        }
        if (severity === "HEALTHY") {
            return "severity-healthy";
        }
        return "severity-neutral";
    }

    function alertDetailUrl(alertId) {
        var encodedId = encodeURIComponent(alertId);
        return "/alerts/" + encodedId + "?id=" + encodedId;
    }

    function isListPage() {
        return Boolean(document.getElementById("alerts-table-body"));
    }

    function isDetailPage() {
        return Boolean(document.getElementById("detail-content"));
    }

    function updateText(id, value) {
        var element = document.getElementById(id);
        if (!element) {
            return;
        }
        element.textContent = value;
    }

    function setListUpdated() {
        updateText("alerts-last-updated", new Date().toLocaleTimeString());
    }

    function setDetailUpdated() {
        updateText("detail-last-updated", new Date().toLocaleTimeString());
    }

    function setFilterButtons() {
        document.querySelectorAll(".filter-btn[data-severity]").forEach(function (button) {
            button.addEventListener("click", function () {
                var severity = button.getAttribute("data-severity") || "ALL";
                listState.severityFilter = severity;
                updateFilterButtonStyles();
                renderAlertsListTable();
            });
        });
    }

    function updateFilterButtonStyles() {
        document.querySelectorAll(".filter-btn[data-severity]").forEach(function (button) {
            var severity = button.getAttribute("data-severity") || "ALL";
            if (severity === listState.severityFilter) {
                button.classList.add("active");
            } else {
                button.classList.remove("active");
            }
        });
    }

    function filteredAlerts() {
        if (listState.severityFilter === "ALL") {
            return listState.alerts;
        }
        return listState.alerts.filter(function (alert) {
            return alert.severity === listState.severityFilter;
        });
    }

    function renderAlertsListTable() {
        var tableBody = document.getElementById("alerts-table-body");
        var emptyState = document.getElementById("alerts-empty");
        var tableWrap = document.getElementById("alerts-table-wrap");
        var visibleCount = document.getElementById("alerts-visible-count");
        var totalCount = document.getElementById("alerts-total-count");

        if (!tableBody || !emptyState || !tableWrap || !visibleCount || !totalCount) {
            return;
        }

        var visibleAlerts = filteredAlerts();

        visibleCount.textContent = String(visibleAlerts.length);
        totalCount.textContent = String(listState.alerts.length);

        if (!visibleAlerts.length) {
            tableBody.innerHTML = "";
            tableWrap.classList.add("hidden");
            emptyState.classList.remove("hidden");
            return;
        }

        emptyState.classList.add("hidden");
        tableWrap.classList.remove("hidden");

        var rows = visibleAlerts.map(function (alert) {
            var isNew = listState.hasRendered && !listState.knownAlertIds.has(alert.id);
            var rowClass = isNew ? "clickable new-alert" : "clickable";
            var notes = alert.notes || "-";

            return ""
                + "<tr class='" + rowClass + "' data-alert-url='" + alertDetailUrl(alert.id) + "'>"
                + "<td>" + escapeHtml(formatDateTime(alert.timestamp)) + "</td>"
                + "<td class='font-mono text-xs text-slate-300'>" + escapeHtml(alert.id) + "</td>"
                + "<td>" + escapeHtml(alert.pipeline_name || "-") + "</td>"
                + "<td>" + escapeHtml(alert.column_name || "-") + "</td>"
                + "<td>" + escapeHtml(alert.alert_type || "-") + "</td>"
                + "<td><span class='severity-badge " + severityClass(alert.severity) + "'>" + escapeHtml(alert.severity || "UNKNOWN") + "</span></td>"
                + "<td>" + escapeHtml(formatScore(alert.score)) + "</td>"
                + "<td title='" + escapeHtml(alert.details || "") + "'>" + escapeHtml(truncateText(alert.details || "-", 100)) + "</td>"
                + "<td>" + (alert.acknowledged ? "Yes" : "No") + "</td>"
                + "<td>" + escapeHtml(truncateText(notes, 80)) + "</td>"
                + "</tr>";
        });

        tableBody.innerHTML = rows.join("");

        tableBody.querySelectorAll("tr[data-alert-url]").forEach(function (row) {
            row.addEventListener("click", function () {
                var url = row.getAttribute("data-alert-url");
                if (url) {
                    window.location.href = url;
                }
            });
        });

        listState.knownAlertIds = new Set(listState.alerts.map(function (alert) {
            return alert.id;
        }));
        listState.hasRendered = true;
    }

    function refreshAlertsList() {
        return fetchJson("/api/alerts?limit=500").then(function (alerts) {
            listState.alerts = alerts;
            renderAlertsListTable();
            setListUpdated();
        }).catch(function (error) {
            console.error("Alerts list refresh failed", error);
        });
    }

    function resolveAlertIdFromUrl() {
        var params = new URLSearchParams(window.location.search);
        var fromQuery = params.get("id");
        if (fromQuery) {
            return fromQuery;
        }

        var pathParts = window.location.pathname.split("/").filter(function (part) {
            return part.length > 0;
        });

        if (pathParts.length >= 2 && pathParts[0] === "alerts") {
            return decodeURIComponent(pathParts[1]);
        }

        return "";
    }

    function parseDistributionPayload(detailsText) {
        if (!detailsText) {
            return null;
        }

        var parsed;
        try {
            parsed = JSON.parse(detailsText);
        } catch (error) {
            return null;
        }

        var baseline = parsed.baseline || parsed.baselineData || parsed.reference;
        var current = parsed.current || parsed.currentData || parsed.observed;

        if (!Array.isArray(baseline) || !Array.isArray(current)) {
            return null;
        }

        return {
            baseline: baseline,
            current: current
        };
    }

    function renderDetailChart(alert) {
        var chartCard = document.getElementById("distribution-chart-card");
        if (!chartCard) {
            return;
        }

        if (alert.alert_type !== "DISTRIBUTION_SHIFT") {
            chartCard.classList.add("hidden");
            return;
        }

        var payload = parseDistributionPayload(alert.details);
        if (!payload || typeof window.renderDistributionChart !== "function") {
            chartCard.classList.add("hidden");
            return;
        }

        chartCard.classList.remove("hidden");
        window.renderDistributionChart("distribution-chart", payload.baseline, payload.current);
    }

    function applyAcknowledgedState(alert) {
        var ackStatus = document.getElementById("detail-ack-status");
        var notesInput = document.getElementById("acknowledge-notes");
        var ackButton = document.getElementById("acknowledge-btn");

        if (!ackStatus || !notesInput || !ackButton) {
            return;
        }

        if (alert.acknowledged) {
            ackStatus.textContent = "Acknowledged";
            ackStatus.className = "text-sm text-green-300";
            ackButton.disabled = true;
            ackButton.textContent = "Acknowledged";
        } else {
            ackStatus.textContent = "Pending";
            ackStatus.className = "text-sm text-slate-300";
            ackButton.disabled = false;
            ackButton.textContent = "Acknowledge";
        }

        if (!notesInput.value && alert.notes) {
            notesInput.value = alert.notes;
        }
    }

    function renderAlertDetail(alert) {
        var detailContent = document.getElementById("detail-content");
        var detailError = document.getElementById("detail-error");
        var badge = document.getElementById("detail-severity-badge");

        if (!detailContent || !detailError || !badge) {
            return;
        }

        detailError.classList.add("hidden");
        detailContent.classList.remove("hidden");

        updateText("detail-id", alert.id || "-");
        updateText("detail-time", formatDateTime(alert.timestamp));
        updateText("detail-pipeline", alert.pipeline_name || "-");
        updateText("detail-column", alert.column_name || "-");
        updateText("detail-type", alert.alert_type || "-");
        updateText("detail-score", formatScore(alert.score));
        updateText("detail-details", alert.details || "-");

        badge.textContent = alert.severity || "UNKNOWN";
        badge.className = "severity-badge severity-large " + severityClass(alert.severity);

        applyAcknowledgedState(alert);
        renderDetailChart(alert);
        setDetailUpdated();
    }

    function showDetailError(message) {
        var detailContent = document.getElementById("detail-content");
        var detailError = document.getElementById("detail-error");
        if (!detailContent || !detailError) {
            return;
        }
        detailContent.classList.add("hidden");
        detailError.textContent = message;
        detailError.classList.remove("hidden");
    }

    function refreshAlertDetail() {
        if (!detailState.alertId) {
            showDetailError("Missing alert id in URL. Use ?id=<alert_id>.");
            return Promise.resolve();
        }

        return fetchJson("/api/alerts/" + encodeURIComponent(detailState.alertId)).then(function (alert) {
            renderAlertDetail(alert);
        }).catch(function (error) {
            console.error("Alert detail refresh failed", error);
            showDetailError("Alert not found.");
        });
    }

    function acknowledgeCurrentAlert() {
        var notesInput = document.getElementById("acknowledge-notes");
        var message = document.getElementById("acknowledge-message");
        var ackButton = document.getElementById("acknowledge-btn");

        if (!notesInput || !message || !ackButton || !detailState.alertId) {
            return;
        }

        ackButton.disabled = true;
        message.textContent = "Saving acknowledgement...";

        fetchJson("/api/alerts/" + encodeURIComponent(detailState.alertId) + "/acknowledge", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Accept": "application/json"
            },
            body: JSON.stringify({
                notes: notesInput.value || ""
            })
        }).then(function () {
            message.textContent = "Alert acknowledged.";
            return refreshAlertDetail();
        }).catch(function (error) {
            console.error("Acknowledgement failed", error);
            ackButton.disabled = false;
            message.textContent = "Failed to acknowledge alert.";
        });
    }

    function initAlertsListPage() {
        setFilterButtons();
        updateFilterButtonStyles();
        refreshAlertsList();
        listState.timer = window.setInterval(refreshAlertsList, REFRESH_INTERVAL_MS);
    }

    function initAlertDetailPage() {
        detailState.alertId = resolveAlertIdFromUrl();

        var acknowledgeButton = document.getElementById("acknowledge-btn");
        if (acknowledgeButton) {
            acknowledgeButton.addEventListener("click", acknowledgeCurrentAlert);
        }

        refreshAlertDetail();
        detailState.timer = window.setInterval(refreshAlertDetail, REFRESH_INTERVAL_MS);
    }

    document.addEventListener("DOMContentLoaded", function () {
        if (isListPage()) {
            initAlertsListPage();
        }
        if (isDetailPage()) {
            initAlertDetailPage();
        }
    });

    window.addEventListener("beforeunload", function () {
        if (listState.timer) {
            window.clearInterval(listState.timer);
        }
        if (detailState.timer) {
            window.clearInterval(detailState.timer);
        }
    });
})();
