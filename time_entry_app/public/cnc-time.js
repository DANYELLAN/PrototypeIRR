let deferredInstallPrompt = null;
const INSTALL_FLAG_KEY = "cnc_time_app_installed";

function hasInstallFlag() {
  try {
    return window.localStorage.getItem(INSTALL_FLAG_KEY) === "true";
  } catch {
    return false;
  }
}

function setInstallFlag() {
  try {
    window.localStorage.setItem(INSTALL_FLAG_KEY, "true");
  } catch {
    // Ignore storage restrictions; standalone detection will still work.
  }
}

function hasInstalledLaunchFlag() {
  try {
    return new URLSearchParams(window.location.search).get("installed") === "1";
  } catch {
    return false;
  }
}

function isInstalledApp() {
  return (
    hasInstallFlag() ||
    hasInstalledLaunchFlag() ||
    window.matchMedia("(display-mode: standalone)").matches ||
    window.navigator.standalone === true
  );
}

function hideInstallButton() {
  const button = document.getElementById("install-app-button");
  if (button) button.hidden = true;
}

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  if (isInstalledApp()) return;
  deferredInstallPrompt = event;
  const button = document.getElementById("install-app-button");
  if (button) button.hidden = false;
});

window.addEventListener("appinstalled", () => {
  setInstallFlag();
  deferredInstallPrompt = null;
  hideInstallButton();
});

window.addEventListener("load", () => {
  if (hasInstalledLaunchFlag()) {
    setInstallFlag();
    if (window.history.replaceState) {
      window.history.replaceState({}, document.title, window.location.pathname);
    }
  }

  const headerClock = document.getElementById("header_clock");
  if (headerClock) {
    const updateHeaderClock = () => {
      headerClock.textContent = new Date().toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      });
    };
    updateHeaderClock();
    window.setInterval(updateHeaderClock, 1000);
  }

  const installButton = document.getElementById("install-app-button");
  if (installButton) {
    if (isInstalledApp()) {
      hideInstallButton();
    }
    installButton.addEventListener("click", async () => {
      if (!deferredInstallPrompt) return;
      deferredInstallPrompt.prompt();
      const choice = await deferredInstallPrompt.userChoice;
      if (choice?.outcome === "accepted") {
        setInstallFlag();
      }
      deferredInstallPrompt = null;
      installButton.hidden = true;
    });
  }

  const adpInput = document.getElementById("adp_number_input");
  const employeeMatch = document.getElementById("employee_match");

  if (adpInput && employeeMatch) {
    let lookupTimer = null;
    let activeLookup = null;

    const resetMatchState = () => {
      employeeMatch.textContent = "Employee match will appear here.";
      employeeMatch.classList.remove("success", "warning");
    };

    const setMatchState = (message, kind) => {
      employeeMatch.textContent = message;
      employeeMatch.classList.toggle("success", kind === "success");
      employeeMatch.classList.toggle("warning", kind === "warning");
    };

    const matchMessage = (value) => {
      const trimmed = String(value ?? "").trim();
      if (!trimmed) {
        if (activeLookup && activeLookup.readyState !== 4) {
          activeLookup.abort();
        }
        resetMatchState();
        return;
      }

      if (activeLookup && activeLookup.readyState !== 4) {
        activeLookup.abort();
      }

      const xhr = new XMLHttpRequest();
      activeLookup = xhr;
      xhr.open("GET", `/lookup-employee?adp_number=${encodeURIComponent(trimmed)}`);
      xhr.onload = () => {
        if (xhr !== activeLookup) return;
        try {
          const response = JSON.parse(xhr.responseText || "{}");
          if (response.employee) {
            setMatchState(`Employee: ${response.employee.full_name} (${response.employee.emp_id})`, "success");
          } else {
            setMatchState("No active Ennis employee with app access found for that ADP number.", "warning");
          }
        } catch {
          setMatchState("No active Ennis employee with app access found for that ADP number.", "warning");
        }
      };
      xhr.onerror = () => {
        if (xhr !== activeLookup) return;
        setMatchState("No active Ennis employee with app access found for that ADP number.", "warning");
      };
      xhr.abort = xhr.abort || (() => {});
      xhr.send();
    };

    const scheduleLookup = (value) => {
      clearTimeout(lookupTimer);
      const trimmed = String(value ?? "").trim();
      if (!trimmed) {
        resetMatchState();
        return;
      }
      lookupTimer = window.setTimeout(() => matchMessage(trimmed), 160);
    };

    adpInput.addEventListener("input", (event) => scheduleLookup(event.target.value));
    adpInput.addEventListener("change", (event) => scheduleLookup(event.target.value));
    adpInput.addEventListener("blur", (event) => {
      clearTimeout(lookupTimer);
      matchMessage(event.target.value);
    });
  }

  if ("serviceWorker" in navigator) {
    navigator.serviceWorker.register("/public/cnc-time-sw.js").catch(() => {});
  }

  const locationSelects = Array.from(document.querySelectorAll(".location-select"));
  const assetSelects = Array.from(document.querySelectorAll(".asset-select"));
  locationSelects.forEach((locationSelect) => {
    locationSelect.addEventListener("change", () => {
      const currentValue = locationSelect.value;
      assetSelects.forEach((assetSelect) => {
        Array.from(assetSelect.options).forEach((option) => {
          const optionLocation = option.dataset.location;
          option.hidden = Boolean(currentValue) && Boolean(optionLocation) && optionLocation !== currentValue;
        });
      });
    });
    locationSelect.dispatchEvent(new Event("change"));
  });

  const operationSelects = Array.from(document.querySelectorAll(".operation-select"));
  operationSelects.forEach((operationSelect) => {
    const form = operationSelect.closest("form");
    const workOrderSelect = form ? form.querySelector('select[name="production_number"]') : null;
    if (!workOrderSelect) return;

    const syncOperations = () => {
      const workOrder = workOrderSelect.value;
      let visibleSelection = false;
      Array.from(operationSelect.options).forEach((option) => {
        const matches = !workOrder || option.dataset.wo === workOrder;
        option.hidden = !matches;
        if (matches && option.selected) visibleSelection = true;
      });
      if (!visibleSelection) {
        const firstVisible = Array.from(operationSelect.options).find((option) => !option.hidden);
        if (firstVisible) firstVisible.selected = true;
      }
    };

    workOrderSelect.addEventListener("change", syncOperations);
    syncOperations();
  });
});
