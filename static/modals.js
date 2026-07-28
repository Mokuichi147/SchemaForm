// 送信一覧・集計ページで共通のモーダル操作と、フィルター条件の日時入力。
// 対象は id と data-action の規約で拾うため、ページ側は markup を置くだけでよい。
(() => {
  const MODALS = [
    { modal: "filter-modal", open: "open-filter-modal", close: "close-filter-modal" },
    { modal: "download-modal", open: "open-download-modal", close: "close-download-modal" },
  ];

  function show(modal) {
    modal.classList.remove("hidden");
    modal.classList.add("flex");
    document.body.classList.add("overflow-hidden");
  }

  function hide(modal) {
    modal.classList.add("hidden");
    modal.classList.remove("flex");
    document.body.classList.remove("overflow-hidden");
  }

  function setupModals() {
    const opened = [];
    MODALS.forEach((spec) => {
      const modal = document.getElementById(spec.modal);
      if (!modal) return;
      opened.push(modal);
      document.getElementById(spec.open)?.addEventListener("click", () => show(modal));
      document
        .querySelectorAll(`[data-action="${spec.close}"]`)
        .forEach((button) => button.addEventListener("click", () => hide(modal)));
      modal.addEventListener("click", (event) => {
        if (event.target === modal) hide(modal);
      });
      // ダウンロードリンクを押したらモーダルは閉じておく。
      modal.querySelectorAll("a[href]").forEach((link) => {
        link.addEventListener("click", () => hide(modal));
      });
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") opened.forEach(hide);
    });
  }

  // フィルターの日時入力。表示はローカル時刻、送信時に絶対時刻へ変換する。
  function setupDatePickers() {
    if (typeof flatpickr === "undefined") return;
    const form = document.getElementById("filter-form");
    const inputs = document.querySelectorAll('input[data-picker="datetime-local"]');

    inputs.forEach((input) => {
      const options = {
        enableTime: true,
        dateFormat: "Y-m-d\\TH:i",
        altInput: true,
        altFormat: "Y/m/d H:i",
        time_24hr: true,
        locale: "ja",
      };
      const parsed = input.value ? new Date(input.value) : null;
      if (parsed && !Number.isNaN(parsed.getTime())) {
        options.defaultDate = parsed;
      } else {
        input.value = "";
      }
      flatpickr(input, options);
    });

    form?.addEventListener("submit", () => {
      inputs.forEach((input) => {
        if (!input.value) return;
        const date = new Date(input.value);
        input.value = Number.isNaN(date.getTime())
          ? ""
          : date.toISOString().replace("Z", "+00:00");
      });
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    setupModals();
    setupDatePickers();
  });
})();
