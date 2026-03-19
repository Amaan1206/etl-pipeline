"use strict";

(function () {
    var chartRegistry = {};

    function toNumber(value, fallback) {
        var numeric = Number(value);
        return isFinite(numeric) ? numeric : fallback;
    }

    function gaussianDensity(x, mean, std) {
        var z = (x - mean) / std;
        return Math.exp(-0.5 * z * z);
    }

    function renderDistributionChart(canvasId, baseline, alertScore) {
        if (typeof Chart === "undefined") {
            console.error("Chart.js is not loaded.");
            return null;
        }

        var canvas = document.getElementById(canvasId);
        if (!canvas) {
            console.error("Canvas not found:", canvasId);
            return null;
        }

        var mean = toNumber(baseline && baseline.mean, null);
        var std = Math.abs(toNumber(baseline && baseline.std, 0));
        var minValue = toNumber(baseline && baseline.min, null);
        var maxValue = toNumber(baseline && baseline.max, null);

        if (mean === null || minValue === null || maxValue === null) {
            console.warn("Distribution chart skipped: invalid baseline input.");
            return null;
        }

        if (!isFinite(std) || std <= 0) {
            std = Math.max(Math.abs(maxValue - minValue) / 6, 1);
        }

        var chartMin = mean - (3 * std);
        var chartMax = mean + (3 * std);
        var bins = 20;
        var step = (chartMax - chartMin) / bins;
        var labels = [];
        var baselineCurve = [];
        var currentCurve = [];
        var shift = toNumber(alertScore, 0) * 0.1;
        var shiftedMean = mean + shift;

        for (var i = 0; i < bins; i += 1) {
            var x = chartMin + (step * (i + 0.5));
            labels.push(x.toFixed(2));
            baselineCurve.push(gaussianDensity(x, mean, std));
            currentCurve.push(gaussianDensity(x, shiftedMean, std));
        }

        if (chartRegistry[canvasId]) {
            chartRegistry[canvasId].destroy();
        }

        var chartTitle = canvas.getAttribute("data-chart-title") || "Distribution Shift";

        var chart = new Chart(canvas.getContext("2d"), {
            type: "line",
            data: {
                labels: labels,
                datasets: [
                    {
                        label: "Baseline",
                        data: baselineCurve,
                        backgroundColor: "rgba(59, 130, 246, 0.6)",
                        borderColor: "rgba(59, 130, 246, 1)",
                        borderWidth: 2,
                        pointRadius: 0,
                        tension: 0.35,
                        fill: true
                    },
                    {
                        label: "Current Batch (shifted)",
                        data: currentCurve,
                        backgroundColor: "rgba(239, 68, 68, 0.6)",
                        borderColor: "rgba(239, 68, 68, 1)",
                        borderWidth: 2,
                        pointRadius: 0,
                        tension: 0.35,
                        fill: true
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
                    title: {
                        display: true,
                        text: chartTitle,
                        color: "#e2e8f0",
                        font: {
                            size: 14,
                            weight: "600"
                        }
                    },
                    legend: {
                        labels: {
                            color: "#e2e8f0"
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function (context) {
                                return context.dataset.label + ": " + context.parsed.y.toFixed(4);
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: "Value range",
                            color: "#94a3b8"
                        },
                        ticks: {
                            color: "#94a3b8",
                            maxRotation: 0,
                            minRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 10
                        },
                        grid: {
                            color: "rgba(148, 163, 184, 0.12)"
                        }
                    },
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: "Probability density",
                            color: "#94a3b8"
                        },
                        ticks: {
                            precision: 4,
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
