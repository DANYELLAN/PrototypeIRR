let deferredInstallPrompt = null;

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  deferredInstallPrompt = event;
  const button = document.getElementById("install-app-button");
  if (button) button.hidden = false;
});

window.addEventListener("load", () => {
  const installButton = document.getElementById("install-app-button");
  if (installButton) {
    installButton.addEventListener("click", async () => {
      if (!deferredInstallPrompt) return;
      deferredInstallPrompt.prompt();
      await deferredInstallPrompt.userChoice;
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
            setMatchState("No active Ennis machinist found for that ADP number.", "warning");
          }
        } catch {
          setMatchState("No active Ennis machinist found for that ADP number.", "warning");
        }
      };
      xhr.onerror = () => {
        if (xhr !== activeLookup) return;
        setMatchState("No active Ennis machinist found for that ADP number.", "warning");
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
});
