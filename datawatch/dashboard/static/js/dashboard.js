"use strict";

(function () {
    var REFRESH_INTERVAL_MS = 30000;
    var knownAlertIds = new Set();
    var knownPipelines = new Set();
    var hasRenderedOnce = false;
    var refreshTimer = null;
    var currentPipeline = null;

    function fetchJson(url) {
        return fetch(url, {
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

    function updateLastUpdated() {
        var target = document.getElementById("last-updated");
        if (!target) {
            return;
        }
        target.textContent = new Date().toLocaleTimeString();
    }

    function buildApiUrl(basePath, params) {
        var query = [];
        Object.keys(params).forEach(function (key) {
            var value = params[key];
            if (value === null || value === undefined || value === "") {
                return;
            }
            query.push(encodeURIComponent(key) + "=" + encodeURIComponent(String(value)));
        });
        if (!query.length) {
            return basePath;
        }
        return basePath + "?" + query.join("&");
    }

    function renderStats(stats) {
        document.getElementById("stat-total").textContent = String(stats.total || 0);
        document.getElementById("stat-critical").textContent = String(stats.critical || 0);
        document.getElementById("stat-warning").textContent = String(stats.warning || 0);
        document.getElementById("stat-last24h").textContent = String(stats.last_24h || 0);
    }

    function renderPipelineFilter(alerts) {
        var select = document.getElementById("pipeline-filter");
        if (!select) {
            return;
        }

        alerts.forEach(function (alert) {
            if (alert && alert.pipeline_name) {
                knownPipelines.add(alert.pipeline_name);
            }
        });

        if (currentPipeline) {
            knownPipelines.add(currentPipeline);
        }

        var pipelines = Array.from(knownPipelines).sort();
        var options = ["<option value=''>All Pipelines</option>"];

        pipelines.forEach(function (pipelineName) {
            options.push(
                "<option value='" + escapeHtml(pipelineName) + "'>" + escapeHtml(pipelineName) + "</option>"
            );
        });

        select.innerHTML = options.join("");
        select.value = currentPipeline || "";
    }

    function alertDetailUrl(alertId) {
        var encodedId = encodeURIComponent(alertId);
        return "/alerts/" + encodedId + "?id=" + encodedId;
    }

    function renderRecentAlerts(alerts) {
        var body = document.getElementById("recent-alerts-body");
        var empty = document.getElementById("recent-alerts-empty");
        var tableWrap = document.getElementById("recent-alerts-table-wrap");
        var count = document.getElementById("recent-alert-count");

        count.textContent = alerts.length + " alerts";

        if (!alerts.length) {
            body.innerHTML = "";
            tableWrap.classList.add("hidden");
            empty.classList.remove("hidden");
            knownAlertIds = new Set();
            hasRenderedOnce = true;
            return;
        }

        empty.classList.add("hidden");
        tableWrap.classList.remove("hidden");

        var rows = alerts.map(function (alert) {
            var isNew = hasRenderedOnce && !knownAlertIds.has(alert.id);
            var rowClass = isNew ? "clickable new-alert" : "clickable";
            return ""
                + "<tr class='" + rowClass + "' data-alert-url='" + alertDetailUrl(alert.id) + "'>"
                + "<td>" + escapeHtml(formatDateTime(alert.timestamp)) + "</td>"
                + "<td>" + escapeHtml(alert.pipeline_name || "-") + "</td>"
                + "<td>" + escapeHtml(alert.column_name || "-") + "</td>"
                + "<td>" + escapeHtml(alert.alert_type || "-") + "</td>"
                + "<td><span class='severity-badge " + severityClass(alert.severity) + "'>" + escapeHtml(alert.severity || "UNKNOWN") + "</span></td>"
                + "<td>" + escapeHtml(formatScore(alert.score)) + "</td>"
                + "</tr>";
        });

        body.innerHTML = rows.join("");

        body.querySelectorAll("tr[data-alert-url]").forEach(function (row) {
            row.addEventListener("click", function () {
                var url = row.getAttribute("data-alert-url");
                if (url) {
                    window.location.href = url;
                }
            });
        });

        knownAlertIds = new Set(alerts.map(function (alert) {
            return alert.id;
        }));
        hasRenderedOnce = true;
    }

    function pipelineStatusFromAlerts(pipelineAlerts) {
        if (!pipelineAlerts.length) {
            return "HEALTHY";
        }
        if (pipelineAlerts.some(function (alert) { return alert.severity === "CRITICAL"; })) {
            return "CRITICAL";
        }
        if (pipelineAlerts.some(function (alert) { return alert.severity === "WARNING"; })) {
            return "WARNING";
        }
        return "HEALTHY";
    }

    function renderPipelineHealth(alerts, monitors) {
        var grid = document.getElementById("pipeline-health-grid");
        var empty = document.getElementById("pipeline-health-empty");
        var count = document.getElementById("pipeline-count");

        var pipelineSet = new Set();

        monitors.forEach(function (monitor) {
            if (monitor.pipeline_id) {
                pipelineSet.add(monitor.pipeline_id);
            }
        });

        alerts.forEach(function (alert) {
            if (alert.pipeline_name) {
                pipelineSet.add(alert.pipeline_name);
            }
        });

        var pipelines = Array.from(pipelineSet).sort();

        count.textContent = pipelines.length + " pipelines";

        if (!pipelines.length) {
            grid.innerHTML = "";
            empty.classList.remove("hidden");
            return;
        }

        empty.classList.add("hidden");

        var cards = pipelines.map(function (pipeline) {
            var pipelineAlerts = alerts.filter(function (alert) {
                return alert.pipeline_name === pipeline;
            });
            var status = pipelineStatusFromAlerts(pipelineAlerts);
            var badgeClass = severityClass(status);
            var lastAlert = pipelineAlerts[0];
            var subtitle = pipelineAlerts.length
                ? pipelineAlerts.length + " alert(s) | Last: " + formatDateTime(lastAlert.timestamp)
                : "No active alerts";

            return ""
                + "<article class='dw-card p-4'>"
                + "<div class='mb-3 flex items-center justify-between gap-2'>"
                + "<h3 class='truncate text-base font-semibold text-slate-100'>" + escapeHtml(pipeline) + "</h3>"
                + "<span class='severity-badge " + badgeClass + "'>" + escapeHtml(status) + "</span>"
                + "</div>"
                + "<p class='text-xs text-slate-400'>" + escapeHtml(subtitle) + "</p>"
                + "</article>";
        });

        grid.innerHTML = cards.join("");
    }

    function fetchAlerts() {
        return fetchJson(
            buildApiUrl("/api/alerts", {
                limit: 100,
                pipeline: currentPipeline
            })
        );
    }

    function fetchStats() {
        return fetchJson(
            buildApiUrl("/api/alerts/stats", {
                pipeline: currentPipeline
            })
        );
    }

    function filterByPipeline(name) {
        currentPipeline = name || null;
        refreshDashboard();
    }

    function refreshDashboard() {
        return Promise.all([
            fetchStats(),
            fetchAlerts(),
            fetchJson("/api/monitors")
        ]).then(function (results) {
            var stats = results[0];
            var alerts = results[1];
            var monitors = results[2];

            renderPipelineFilter(alerts);
            renderStats(stats);
            renderRecentAlerts(alerts);
            renderPipelineHealth(alerts, monitors);
            updateLastUpdated();
        }).catch(function (error) {
            console.error("Dashboard refresh failed", error);
        });
    }

    document.addEventListener("DOMContentLoaded", function () {
        var pipelineFilter = document.getElementById("pipeline-filter");
        if (pipelineFilter) {
            pipelineFilter.addEventListener("change", function (event) {
                filterByPipeline(event.target.value || null);
            });
        }

        refreshDashboard();
        refreshTimer = window.setInterval(refreshDashboard, REFRESH_INTERVAL_MS);
    });

    window.addEventListener("beforeunload", function () {
        if (refreshTimer) {
            window.clearInterval(refreshTimer);
        }
    });
})();
