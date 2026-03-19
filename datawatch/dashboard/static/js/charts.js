"use strict";

(function () {
    var chartRegistry = {};

    function toNumberArray(input) {
        if (!Array.isArray(input)) {
            return [];
        }

        return input
            .map(function (value) {
                if (typeof value === "number") {
                    return value;
                }
                if (value && typeof value === "object" && typeof value.value === "number") {
                    return value.value;
                }
                var numeric = Number(value);
                return isFinite(numeric) ? numeric : null;
            })
            .filter(function (value) {
                return value !== null;
            });
    }

    function histogram(values, binEdges) {
        var counts = new Array(binEdges.length - 1).fill(0);

        values.forEach(function (value) {
            for (var i = 0; i < binEdges.length - 1; i += 1) {
                var left = binEdges[i];
                var right = binEdges[i + 1];
                var isLastBin = i === binEdges.length - 2;

                if ((value >= left && value < right) || (isLastBin && value === right)) {
                    counts[i] += 1;
                    break;
                }
            }
        });

        return counts;
    }

    function buildBinEdges(minValue, maxValue, bins) {
        var edges = [];
        var width = (maxValue - minValue) / bins;

        for (var i = 0; i <= bins; i += 1) {
            edges.push(minValue + (width * i));
        }

        return edges;
    }

    function formatLabel(value) {
        return Number(value).toFixed(2);
    }

    function renderDistributionChart(canvasId, baselineData, currentData) {
        if (typeof Chart === "undefined") {
            console.error("Chart.js is not loaded.");
            return null;
        }

        var canvas = document.getElementById(canvasId);
        if (!canvas) {
            console.error("Canvas not found:", canvasId);
            return null;
        }

        var baselineValues = toNumberArray(baselineData);
        var currentValues = toNumberArray(currentData);

        if (!baselineValues.length || !currentValues.length) {
            console.warn("Distribution chart skipped: missing baseline/current data.");
            return null;
        }

        var allValues = baselineValues.concat(currentValues);
        var minValue = Math.min.apply(null, allValues);
        var maxValue = Math.max.apply(null, allValues);

        if (minValue === maxValue) {
            maxValue = minValue + 1;
        }

        var bins = 12;
        var edges = buildBinEdges(minValue, maxValue, bins);
        var labels = [];

        for (var i = 0; i < edges.length - 1; i += 1) {
            labels.push(formatLabel(edges[i]) + " - " + formatLabel(edges[i + 1]));
        }

        var baselineCounts = histogram(baselineValues, edges);
        var currentCounts = histogram(currentValues, edges);

        if (chartRegistry[canvasId]) {
            chartRegistry[canvasId].destroy();
        }

        var chart = new Chart(canvas.getContext("2d"), {
            type: "bar",
            data: {
                labels: labels,
                datasets: [
                    {
                        label: "Baseline",
                        data: baselineCounts,
                        backgroundColor: "rgba(59, 130, 246, 0.5)",
                        borderColor: "rgba(59, 130, 246, 1)",
                        borderWidth: 1,
                        grouped: false,
                        barThickness: 18
                    },
                    {
                        label: "Current",
                        data: currentCounts,
                        backgroundColor: "rgba(239, 68, 68, 0.5)",
                        borderColor: "rgba(239, 68, 68, 1)",
                        borderWidth: 1,
                        grouped: false,
                        barThickness: 12
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                interaction: {
                    mode: "index",
                    intersect: false
                },
                plugins: {
                    legend: {
                        labels: {
                            color: "#e2e8f0"
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function (context) {
                                return context.dataset.label + ": " + context.parsed.y;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: {
                            color: "#94a3b8",
                            maxRotation: 60,
                            minRotation: 60,
                            autoSkip: true,
                            maxTicksLimit: 8
                        },
                        grid: {
                            color: "rgba(148, 163, 184, 0.12)"
                        }
                    },
                    y: {
                        beginAtZero: true,
                        ticks: {
                            precision: 0,
                            color: "#94a3b8"
                        },
                        grid: {
                            color: "rgba(148, 163, 184, 0.12)"
                        }
                    }
                }
            }
        });

        chartRegistry[canvasId] = chart;
        return chart;
    }

    window.renderDistributionChart = renderDistributionChart;
})();
