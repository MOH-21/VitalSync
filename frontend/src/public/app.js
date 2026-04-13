(function () {
  "use strict";

  const userSelect = document.getElementById("user-select");
  const tabs = document.querySelectorAll(".tab");
  const loadingEl = document.getElementById("loading");
  const errorEl = document.getElementById("error");
  const contentEl = document.getElementById("content");
  const placeholderEl = document.getElementById("placeholder");
  const summaryTbody = document.querySelector("#summary-table tbody");
  const recommendationEl = document.getElementById("recommendation");

  let activeMetric = "heart_rate";

  // --- Initialization ---

  async function loadUsers() {
    try {
      const res = await fetch("/api/users");
      if (!res.ok) throw new Error("Failed to load patients");
      const users = await res.json();
      users.forEach(function (user) {
        const opt = document.createElement("option");
        const userId = typeof user === "string" ? user : (user.user_id || user.id);
        opt.value = userId;
        opt.textContent = "Patient " + userId.replace("user_", "");
        userSelect.appendChild(opt);
      });
      userSelect.disabled = false;
    } catch (err) {
      showError("Unable to load patient list. Is the API running?");
    }
  }

  // --- UI helpers ---

  function showOnly(el) {
    [loadingEl, errorEl, contentEl, placeholderEl].forEach(function (s) {
      s.classList.add("hidden");
    });
    if (el) el.classList.remove("hidden");
  }

  function showError(message) {
    errorEl.textContent = message;
    showOnly(errorEl);
  }

  // --- Data fetching ---

  async function fetchRecommendation(userId, metricName) {
    showOnly(loadingEl);

    try {
      const url =
        "/api/recommendations?user_id=" +
        encodeURIComponent(userId) +
        "&metric_name=" +
        encodeURIComponent(metricName);

      const res = await fetch(url);
      if (!res.ok) {
        const body = await res.json().catch(function () {
          return {};
        });
        throw new Error(body.detail || "Request failed (" + res.status + ")");
      }

      const data = await res.json();
      renderSummary(data.data_summary);
      renderRecommendation(data.recommendation);
      showOnly(contentEl);
    } catch (err) {
      showError(err.message || "Something went wrong.");
    }
  }

  // --- Rendering ---

  var STAT_LABELS = {
    min: "Minimum",
    max: "Maximum",
    avg: "Average",
    latest: "Latest",
    trend: "Trend",
    count: "Data Points",
    std_dev: "Std Dev",
  };

  function renderSummary(summary) {
    summaryTbody.innerHTML = "";

    if (!summary || typeof summary !== "object") {
      var row = summaryTbody.insertRow();
      row.insertCell().textContent = "No data";
      row.insertCell().textContent = "-";
      return;
    }

    // Show known keys first in order, then the rest
    var orderedKeys = ["min", "max", "avg", "latest", "trend", "count", "std_dev"];
    var allKeys = Object.keys(summary);
    var remaining = allKeys.filter(function (k) {
      return orderedKeys.indexOf(k) === -1;
    });
    var keys = orderedKeys.filter(function (k) {
      return summary[k] !== undefined;
    }).concat(remaining);

    keys.forEach(function (key) {
      var row = summaryTbody.insertRow();
      row.insertCell().textContent = STAT_LABELS[key] || key;
      var val = summary[key];
      if (typeof val === "number") {
        val = Number.isInteger(val) ? val : val.toFixed(2);
      }
      row.insertCell().textContent = val;
    });
  }

  function renderRecommendation(text) {
    recommendationEl.textContent = text || "No recommendation available.";
  }

  // --- Event listeners ---

  tabs.forEach(function (btn) {
    btn.addEventListener("click", function () {
      tabs.forEach(function (t) {
        t.classList.remove("active");
        t.setAttribute("aria-selected", "false");
      });
      btn.classList.add("active");
      btn.setAttribute("aria-selected", "true");
      activeMetric = btn.dataset.metric;

      if (userSelect.value) {
        fetchRecommendation(userSelect.value, activeMetric);
      }
    });
  });

  userSelect.addEventListener("change", function () {
    if (userSelect.value) {
      fetchRecommendation(userSelect.value, activeMetric);
    } else {
      showOnly(placeholderEl);
    }
  });

  // --- Start ---
  loadUsers();
})();
