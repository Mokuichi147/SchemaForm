  const SF_FILE_INFOS = (() => {
    try {
      const el = document.getElementById("sf-file-infos");
      return el ? JSON.parse(el.textContent || "{}") : {};
    } catch (_) {
      return {};
    }
  })();

  function preventSubmitOnEnter(form) {
    if (!form) return;
    let allowSubmitByButton = false;
    let allowSubmitTimer = 0;

    function markExplicitSubmit() {
      allowSubmitByButton = true;
      if (allowSubmitTimer) {
        window.clearTimeout(allowSubmitTimer);
      }
      allowSubmitTimer = window.setTimeout(() => {
        allowSubmitByButton = false;
        allowSubmitTimer = 0;
      }, 800);
    }

    function isSubmitControl(element) {
      if (element instanceof HTMLButtonElement) {
        const type = (element.getAttribute("type") || "submit").toLowerCase();
        return type === "submit" && element.form === form;
      }
      if (element instanceof HTMLInputElement) {
        const type = (element.type || "").toLowerCase();
        return type === "submit" && element.form === form;
      }
      return false;
    }

    form.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const control = target.closest("button, input");
      if (!(control instanceof HTMLElement)) return;
      if (isSubmitControl(control)) {
        markExplicitSubmit();
      }
    });

    form.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") return;
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      if (target.tagName === "TEXTAREA" || target.isContentEditable) return;
      if (isSubmitControl(target)) {
        markExplicitSubmit();
        return;
      }
      const isImeConfirm = event.isComposing || event.keyCode === 229 || event.which === 229;
      if (target instanceof HTMLInputElement || target instanceof HTMLSelectElement) {
        event.preventDefault();
        if (isImeConfirm) {
          event.stopPropagation();
        }
      }
    });

    form.addEventListener("submit", (event) => {
      if (!allowSubmitByButton) {
        event.preventDefault();
        return;
      }
      allowSubmitByButton = false;
      if (allowSubmitTimer) {
        window.clearTimeout(allowSubmitTimer);
        allowSubmitTimer = 0;
      }
    });
  }

  function initPicker(input) {
    if (!input) return;
    if (typeof flatpickr === "undefined") return;
    const kind = input.dataset.picker;
    if (!kind || input._flatpickr) return;
    const options = { locale: "ja" };
    if (kind === "datetime-local") {
      options.enableTime = true;
      options.dateFormat = "Y-m-d\\TH:i";
      options.altInput = true;
      options.altFormat = "Y/m/d H:i";
      options.time_24hr = true;
    } else if (kind === "date") {
      options.dateFormat = "Y-m-d";
      options.altInput = true;
      options.altFormat = "Y/m/d";
    } else if (kind === "time") {
      options.enableTime = true;
      options.noCalendar = true;
      options.dateFormat = "H:i";
      options.time_24hr = true;
      options.altInput = true;
      options.altFormat = "H:i";
    }
    const rawValue = input.value;
    if (rawValue) {
      const parsed = new Date(rawValue);
      if (!Number.isNaN(parsed.getTime())) {
        options.defaultDate = parsed;
      }
    }
    flatpickr(input, options);
  }

  const CHOICE_SEARCH_THRESHOLD = 6;

  function snapshotSelectOptions(select) {
    return Array.from(select.options).map((option) => ({
      value: option.value,
      text: option.textContent || "",
      disabled: option.disabled,
      dataset: { ...option.dataset },
    }));
  }

  function buildSelectOption(meta) {
    const option = document.createElement("option");
    option.value = meta.value;
    option.textContent = meta.text;
    option.disabled = Boolean(meta.disabled);
    Object.entries(meta.dataset || {}).forEach(([key, value]) => {
      option.dataset[key] = String(value);
    });
    return option;
  }

  function applyChoiceFilter(select, searchInput, excludedValues) {
    const allOptions = select._sfChoiceOptions || [];
    if (allOptions.length === 0) return;
    const query = (searchInput?.value || "").trim().toLowerCase();
    const selectedValue = select.value;
    const excluded = excludedValues || null;
    const filtered = allOptions.filter((meta) => {
      const isPlaceholder = meta.value === "";
      if (isPlaceholder) return true;
      if (selectedValue !== "" && meta.value === selectedValue) return true;
      if (excluded && excluded.has(meta.value)) return false;
      if (!query) return true;
      return meta.text.toLowerCase().includes(query);
    });
    select.replaceChildren(...filtered.map((meta) => buildSelectOption(meta)));
    if (selectedValue !== "") {
      select.value = selectedValue;
    }
    if (select.dataset.role === "master-select") {
      updateMasterDisplay(select);
    }
  }

  function initChoiceSelectSearch(scope) {
    if (!scope || !scope.querySelectorAll) return;
    scope.querySelectorAll("select[data-choice-select='1']").forEach((select) => {
      if (select.dataset.sfChoiceInit === "1") return;
      const allOptions = snapshotSelectOptions(select);
      const optionCount = allOptions.filter((meta) => meta.value !== "").length;
      if (optionCount < CHOICE_SEARCH_THRESHOLD) {
        select.dataset.sfChoiceInit = "1";
        return;
      }
      const searchInput = document.createElement("input");
      searchInput.type = "search";
      searchInput.autocomplete = "off";
      searchInput.spellcheck = false;
      searchInput.placeholder = select.dataset.searchPlaceholder || "検索";
      searchInput.className = "sf-choice-search mb-1 w-full rounded border border-slate-300 px-2 py-1 text-sm";
      searchInput.disabled = select.disabled;
      select._sfChoiceOptions = allOptions;
      select.parentElement?.insertBefore(searchInput, select);
      searchInput.addEventListener("input", () => applyChoiceFilter(select, searchInput));
      select.dataset.sfChoiceInit = "1";
    });
  }

  function parseMasterDisplay(option) {
    const raw = option?.dataset.masterDisplay || "{}";
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed;
      }
      return {};
    } catch (error) {
      return {};
    }
  }

  function normalizeUrlCandidate(rawValue) {
    const value = String(rawValue || "").trim();
    if (!value) return "";
    if (value.startsWith("http://") || value.startsWith("https://")) {
      return value;
    }
    if (value.startsWith("www.")) {
      return `https://${value}`;
    }
    return "";
  }

  function renderMasterFileEntry(container, fileId) {
    const info = SF_FILE_INFOS[fileId] || null;
    const name = info?.name || fileId;
    const kind = info?.kind || "";
    const href = `/files/${encodeURIComponent(fileId)}`;

    const wrapper = document.createElement("div");
    wrapper.className = "space-y-1";

    if (kind === "image") {
      const img = document.createElement("img");
      img.src = href;
      img.alt = name;
      img.loading = "lazy";
      img.className = "max-h-40 max-w-xs rounded border border-slate-200 object-contain cursor-pointer";
      img.dataset.lightbox = href;
      img.dataset.lightboxKind = "image";
      img.dataset.lightboxName = name;
      wrapper.appendChild(img);
    } else if (kind === "video") {
      const thumb = document.createElement("div");
      thumb.className = "max-h-48 max-w-sm flex items-center justify-center bg-slate-100 rounded border border-slate-200 cursor-pointer h-32 w-48";
      thumb.dataset.lightbox = href;
      thumb.dataset.lightboxKind = "video";
      thumb.dataset.lightboxName = name;
      thumb.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" class="h-8 w-8 text-slate-400" fill="currentColor" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg>';
      wrapper.appendChild(thumb);
    } else if (kind === "audio") {
      const audio = document.createElement("audio");
      audio.src = href;
      audio.controls = true;
      audio.preload = "metadata";
      audio.className = "w-full max-w-xs";
      wrapper.appendChild(audio);
    }

    const link = document.createElement("a");
    link.href = href;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    link.className = "block break-all text-xs text-slate-500 underline";
    link.textContent = name;
    wrapper.appendChild(link);

    container.appendChild(wrapper);
  }

  function renderMasterFileValue(valueEl, rawValue) {
    const ids = String(rawValue ?? "")
      .split(",")
      .map((token) => token.trim())
      .filter((token) => token.length > 0);
    if (ids.length === 0) return;
    const wrap = document.createElement("div");
    wrap.className = "flex flex-wrap gap-2";
    ids.forEach((id) => renderMasterFileEntry(wrap, id));
    valueEl.appendChild(wrap);
  }

  function renderMasterDisplayValue(valueEl, rawValue, type) {
    if (!valueEl) return;
    valueEl.replaceChildren();
    const value = String(rawValue ?? "").trim();
    if (!value) return;

    if (type === "file") {
      renderMasterFileValue(valueEl, value);
      return;
    }

    const parts = value
      .split(/,\s+/)
      .map((part) => part.trim())
      .filter((part) => part.length > 0);
    const items = parts.length > 1 ? parts : [value];

    items.forEach((item, index) => {
      if (index > 0) {
        valueEl.append(", ");
      }
      const href = normalizeUrlCandidate(item);
      if (!href) {
        valueEl.append(item);
        return;
      }
      const link = document.createElement("a");
      link.href = href;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.className = "break-all text-sky-700 underline";
      link.textContent = item;
      valueEl.append(link);
    });
  }

  function updateMasterDisplay(select) {
    if (!select) return;
    const wrapper = select.closest("[data-role='master-ref']");
    if (!wrapper) return;
    const panel = wrapper.querySelector("[data-role='master-display']");
    if (!panel) return;
    const rows = Array.from(panel.querySelectorAll("[data-role='master-display-row']"));
    if (rows.length === 0) {
      panel.classList.add("hidden");
      return;
    }
    const option = select.options[select.selectedIndex];
    const displayMap = parseMasterDisplay(option);
    let visibleCount = 0;
    rows.forEach((row) => {
      const key = row.dataset.masterDisplayKey || "";
      const type = row.dataset.masterDisplayType || "";
      const value = String(displayMap[key] ?? "").trim();
      const valueEl = row.querySelector("[data-role='master-display-value']");
      renderMasterDisplayValue(valueEl, value, type);
      const show = value.length > 0;
      row.classList.toggle("hidden", !show);
      if (show) {
        visibleCount += 1;
      }
    });
    panel.classList.toggle("hidden", !select.value || visibleCount === 0);
  }

  function initMasterReference(scope) {
    if (!scope || !scope.querySelectorAll) return;
    scope.querySelectorAll("select[data-role='master-select']").forEach((select) => {
      if (select.dataset.sfMasterInit !== "1") {
        select.dataset.sfMasterInit = "1";
        select.addEventListener("change", () => updateMasterDisplay(select));
      }
      updateMasterDisplay(select);
    });
  }

  function initArrayFieldBlock(block) {
    if (!block || block.dataset.sfArrayInit === "1") return;
    block.dataset.sfArrayInit = "1";
    const template = block.querySelector("template");
    const list = block.querySelector(".array-items");
    const addButton = block.querySelector(".add-item");
    const picker = block.dataset.picker;
    const isUnique = block.dataset.unique === "1";
    let initialData = [];
    try { initialData = JSON.parse(block.dataset.initial || "[]"); } catch (_) { initialData = []; }
    if (!Array.isArray(initialData)) initialData = [];
    if (!template || !list || !addButton) return;

    // 配列要素ごとに検索欄が並ぶと画面を占有するため、配列内の選択フィールドは
    // ブロック上部の共有検索欄でまとめてフィルタリングする。
    let sharedSearch = null;
    const templateSelect = template.content.querySelector("select[data-choice-select='1']");
    if (templateSelect) {
      const optionCount = Array.from(templateSelect.options).filter((o) => o.value !== "").length;
      if (optionCount >= CHOICE_SEARCH_THRESHOLD) {
        sharedSearch = document.createElement("input");
        sharedSearch.type = "search";
        sharedSearch.autocomplete = "off";
        sharedSearch.spellcheck = false;
        sharedSearch.placeholder = templateSelect.dataset.searchPlaceholder || "検索";
        sharedSearch.className = "sf-choice-search mb-2 w-full rounded border border-slate-300 px-2 py-1 text-sm";
        sharedSearch.disabled = addButton.disabled;
        block.insertBefore(sharedSearch, list);
        sharedSearch.addEventListener("input", () => {
          const selects = Array.from(
            list.querySelectorAll("select[data-choice-select='1']")
          );
          const taken = isUnique
            ? new Set(selects.map((s) => s.value).filter((v) => v !== ""))
            : null;
          selects.forEach((select) => {
            let excluded = null;
            if (taken) {
              excluded = new Set(taken);
              if (select.value) excluded.delete(select.value);
            }
            applyChoiceFilter(select, sharedSearch, excluded);
          });
        });
      }
    }

    function updateAddButtonState() {
      if (!sharedSearch) return;
      if (addButton.dataset.sfPermanentDisabled === "1") return;
      const hasUnselected = Array.from(
        list.querySelectorAll("select[data-choice-select='1']")
      ).some((select) => select.value === "");
      addButton.disabled = hasUnselected;
      addButton.classList.toggle("opacity-50", hasUnselected);
      addButton.classList.toggle("cursor-not-allowed", hasUnselected);
    }

    function refreshUniqueOptions() {
      if (!isUnique) return;
      const selects = Array.from(list.querySelectorAll("select[data-choice-select='1']"));
      const taken = new Set(
        selects.map((s) => s.value).filter((v) => v !== "")
      );
      selects.forEach((select) => {
        const own = select.value;
        const excluded = new Set(taken);
        if (own) excluded.delete(own);
        applyChoiceFilter(select, sharedSearch, excluded);
      });
    }

    if (addButton.disabled) {
      addButton.dataset.sfPermanentDisabled = "1";
    }

    function addItem(value) {
      const node = template.content.firstElementChild.cloneNode(true);
      const removeButton = node.querySelector(".remove-item");
      if (removeButton) {
        removeButton.addEventListener("click", () => {
          node.remove();
          updateAddButtonState();
        });
      }
      if (value !== undefined && value !== null) {
        // master参照の場合は select[data-role="master-select"]、その他は最初の入力要素
        const masterSelect = node.querySelector("select[data-role='master-select']");
        const target = masterSelect || node.querySelector("input, select, textarea");
        if (target) {
          target.value = String(value);
        }
      }
      if (picker) {
        const input = node.querySelector("input[data-picker]");
        initPicker(input);
      }
      if (sharedSearch || isUnique) {
        node.querySelectorAll("select[data-choice-select='1']").forEach((select) => {
          if (!select._sfChoiceOptions) {
            select._sfChoiceOptions = snapshotSelectOptions(select);
          }
          select.dataset.sfChoiceInit = "1";
          select.addEventListener("change", () => {
            updateAddButtonState();
            refreshUniqueOptions();
          });
        });
      }
      initChoiceSelectSearch(node);
      initMasterReference(node);
      list.appendChild(node);
      if (sharedSearch) {
        sharedSearch.value = "";
        list
          .querySelectorAll("select[data-choice-select='1']")
          .forEach((select) => applyChoiceFilter(select, sharedSearch));
      }
      refreshUniqueOptions();
      updateAddButtonState();
    }

    if (isUnique) {
      list.addEventListener("click", (event) => {
        if (event.target.closest(".remove-item")) {
          setTimeout(refreshUniqueOptions, 0);
        }
      });
    }

    addButton.addEventListener("click", () => {
      if (addButton.disabled) return;
      addItem();
    });

    if (initialData.length > 0) {
      initialData.forEach((val) => addItem(val));
    } else {
      addItem();
    }
  }

  function initArrayGroupBlock(block) {
    if (!block || block.dataset.sfArrayInit === "1") return;
    block.dataset.sfArrayInit = "1";
    const template = block.querySelector("template");
    const list = block.querySelector(".array-group-items");
    const addButton = block.querySelector(".add-group-item");
    const removeCurrentButton = block.querySelector(".remove-group-item-current");
    const prevButton = block.querySelector(".group-prev");
    const nextButton = block.querySelector(".group-next");
    const paginationLabel = block.querySelector("[data-role='group-pagination']");
    const dotsContainer = block.querySelector("[data-role='group-dots']");
    const indexToken = block.dataset.indexToken || "__INDEX__";
    const slideDurationMs = 280;
    let initialItems = [];
    try { initialItems = JSON.parse(block.dataset.initialItems || "[]"); } catch (_) { initialItems = []; }
    if (!Array.isArray(initialItems)) initialItems = [];

    let counter = 0;
    let currentIndex = 0;
    let isAnimating = false;
    if (!template || !list || !addButton) return;

    function getEntries() {
      return Array.from(list.children).filter((el) => el.classList.contains("array-group-entry"));
    }

    function cleanupSlideStyles(entry) {
      if (!entry) return;
      entry.style.transition = "";
      entry.style.transform = "";
      entry.style.position = "";
      entry.style.inset = "";
      entry.style.width = "";
      entry.style.zIndex = "";
      entry.style.opacity = "";
      entry.style.pointerEvents = "";
    }

    function playAddButtonAnimation() {
      addButton.classList.remove("sf-button-pop");
      void addButton.offsetWidth;
      addButton.classList.add("sf-button-pop");
    }

    function playPageTransition(fromEntry, toEntry, direction, onTransitionEnd = null) {
      if (!fromEntry || !toEntry || fromEntry === toEntry) return;
      const incomingFrom = direction < 0 ? "-100%" : "100%";
      const outgoingTo = direction < 0 ? "100%" : "-100%";
      isAnimating = true;
      if (list._sfSlideTimer) {
        window.clearTimeout(list._sfSlideTimer);
      }
      const maxHeight = Math.max(fromEntry.offsetHeight, toEntry.offsetHeight);
      list.style.position = "relative";
      list.style.minHeight = `${maxHeight}px`;
      list.style.overflow = "hidden";
      fromEntry.classList.remove("hidden");
      toEntry.classList.remove("hidden");
      fromEntry.style.position = "absolute";
      toEntry.style.position = "absolute";
      fromEntry.style.inset = "0";
      toEntry.style.inset = "0";
      fromEntry.style.width = "100%";
      toEntry.style.width = "100%";
      fromEntry.style.zIndex = "1";
      toEntry.style.zIndex = "2";
      fromEntry.style.pointerEvents = "none";
      toEntry.style.pointerEvents = "none";
      fromEntry.style.transition = "none";
      toEntry.style.transition = "none";
      fromEntry.style.transform = "translateX(0)";
      toEntry.style.transform = `translateX(${incomingFrom})`;
      void list.offsetWidth;
      fromEntry.style.transition = `transform ${slideDurationMs}ms cubic-bezier(0.22, 0.61, 0.36, 1)`;
      toEntry.style.transition = `transform ${slideDurationMs}ms cubic-bezier(0.22, 0.61, 0.36, 1)`;
      fromEntry.style.transform = `translateX(${outgoingTo})`;
      toEntry.style.transform = "translateX(0)";
      list._sfSlideTimer = window.setTimeout(() => {
        cleanupSlideStyles(fromEntry);
        cleanupSlideStyles(toEntry);
        if (typeof onTransitionEnd === "function") {
          onTransitionEnd();
        }
        isAnimating = false;
        refreshGroupState();
        list.style.position = "";
        list.style.minHeight = "";
        list.style.overflow = "";
      }, slideDurationMs + 30);
    }

    function renderDots(total, controlsLocked) {
      if (!dotsContainer) return;
      dotsContainer.replaceChildren();
      if (total <= 1) return;
      for (let index = 0; index < total; index += 1) {
        const dot = document.createElement("button");
        dot.type = "button";
        dot.className = "group-page-dot";
        dot.disabled = controlsLocked;
        if (index === currentIndex) {
          dot.classList.add("is-active");
          dot.setAttribute("aria-current", "page");
        } else {
          dot.setAttribute("aria-current", "false");
        }
        dot.setAttribute("aria-label", `${index + 1}件目へ移動`);
        dot.addEventListener("click", () => {
          if (index === currentIndex || isAnimating) return;
          const direction = index > currentIndex ? 1 : -1;
          const fromIndex = currentIndex;
          currentIndex = index;
          refreshGroupState({ animate: true, direction, fromIndex });
        });
        dotsContainer.appendChild(dot);
      }
    }

    function refreshGroupState(options = {}) {
      const { animate = false, direction = 1, fromIndex = currentIndex, onTransitionEnd = null } = options;
      const entries = getEntries();
      const total = entries.length;
      if (total === 0) {
        currentIndex = 0;
      } else if (currentIndex >= total) {
        currentIndex = total - 1;
      } else if (currentIndex < 0) {
        currentIndex = 0;
      }
      if (!isAnimating) {
        entries.forEach((entry, index) => {
          entry.classList.toggle("hidden", index !== currentIndex);
        });
      }
      if (animate && !isAnimating && total > 0 && fromIndex >= 0 && fromIndex < total && fromIndex !== currentIndex) {
        entries.forEach((entry, index) => {
          if (index !== fromIndex && index !== currentIndex) {
            entry.classList.add("hidden");
          }
        });
        playPageTransition(entries[fromIndex], entries[currentIndex], direction, onTransitionEnd);
      }
      if (paginationLabel) {
        if (total > 0) {
          const detailed = `${currentIndex + 1}件目 / 全${total}件`;
          paginationLabel.textContent = `${currentIndex + 1}/${total}`;
          paginationLabel.title = detailed;
          paginationLabel.setAttribute("aria-label", detailed);
        } else {
          paginationLabel.textContent = "0/0";
          paginationLabel.title = "0件目 / 全0件";
          paginationLabel.setAttribute("aria-label", "0件目 / 全0件");
        }
      }
      const controlsLocked = addButton.disabled || isAnimating;
      if (prevButton) {
        prevButton.disabled = controlsLocked || total <= 1 || currentIndex <= 0;
      }
      if (nextButton) {
        nextButton.disabled = controlsLocked || total <= 1 || currentIndex >= total - 1;
      }
      if (removeCurrentButton) {
        removeCurrentButton.disabled = controlsLocked || total <= 0;
      }
      renderDots(total, controlsLocked);
    }

    function bootstrapNode(node) {
      node.querySelectorAll("input[data-picker]").forEach(initPicker);
      node.querySelectorAll(".array-field").forEach(initArrayFieldBlock);
      node.querySelectorAll(".array-group").forEach(initArrayGroupBlock);
      initChoiceSelectSearch(node);
      initMasterReference(node);
    }

    function fillGroupEntryValues(node, data, prefix) {
      if (!data || typeof data !== "object") return;
      Object.entries(data).forEach(([key, value]) => {
        const fieldName = prefix + key;
        const escaped = CSS.escape(fieldName);
        if (Array.isArray(value)) {
          // 配列フィールド (非グループ) の data-initial を設定
          const arrayBlock = node.querySelector('.array-field[data-key="' + escaped + '"]');
          if (arrayBlock) {
            arrayBlock.dataset.initial = JSON.stringify(value);
            return;
          }
          // ネストされた配列グループの data-initial-items を設定
          const groupBlock = node.querySelector('.array-group[data-prefix="' + escaped + '"]');
          if (groupBlock) {
            groupBlock.dataset.initialItems = JSON.stringify(value);
            return;
          }
        } else if (value !== null && typeof value === "object") {
          // ネストされた通常グループ
          fillGroupEntryValues(node, value, fieldName + ".");
        } else {
          const inputs = node.querySelectorAll('[name="' + escaped + '"]');
          inputs.forEach((input) => {
            if (input.type === "checkbox") {
              input.checked = Boolean(value);
            } else if (input.tagName === "SELECT") {
              input.value = String(value ?? "");
            } else {
              input.value = value ?? "";
            }
          });
        }
      });
    }

    function addGroupItem(options = {}) {
      const { animate = false, data = null } = options;
      const previousIndex = currentIndex;
      const node = template.content.firstElementChild.cloneNode(true);
      const idx = counter++;
      node.innerHTML = node.innerHTML.replaceAll(indexToken, String(idx));
      const entryPrefix = block.dataset.prefix + "." + idx + ".";
      if (data) {
        fillGroupEntryValues(node, data, entryPrefix);
      }
      bootstrapNode(node);
      list.appendChild(node);
      currentIndex = getEntries().length - 1;
      refreshGroupState({ animate, direction: 1, fromIndex: previousIndex });
    }

    addButton.addEventListener("click", () => {
      if (isAnimating) return;
      if (addButton.disabled) return;
      playAddButtonAnimation();
      addGroupItem({ animate: true });
    });
    if (removeCurrentButton) {
      removeCurrentButton.addEventListener("click", () => {
        if (isAnimating) return;
        const entries = getEntries();
        if (entries.length === 0) return;
        if (entries.length === 1) {
          const onlyEntry = entries[0];
          if (!onlyEntry) return;
          onlyEntry.remove();
          currentIndex = 0;
          refreshGroupState();
          return;
        }
        const fromIdx = currentIndex;
        const fromEntry = entries[fromIdx];
        const hasNext = fromIdx < entries.length - 1;
        const toIndex = hasNext ? fromIdx + 1 : fromIdx - 1;
        const dir = hasNext ? 1 : -1;
        const toEntry = entries[toIndex];
        if (!fromEntry || !toEntry) return;
        currentIndex = toIndex;
        refreshGroupState({
          animate: true,
          direction: dir,
          fromIndex: fromIdx,
          onTransitionEnd: () => {
            fromEntry.remove();
            if (fromIdx < currentIndex) {
              currentIndex -= 1;
            }
          },
        });
      });
    }
    if (prevButton) {
      prevButton.addEventListener("click", () => {
        if (currentIndex <= 0 || isAnimating) return;
        const fromIdx = currentIndex;
        currentIndex -= 1;
        refreshGroupState({ animate: true, direction: -1, fromIndex: fromIdx });
      });
    }
    if (nextButton) {
      nextButton.addEventListener("click", () => {
        if (isAnimating) return;
        const total = getEntries().length;
        if (currentIndex >= total - 1) return;
        const fromIdx = currentIndex;
        currentIndex += 1;
        refreshGroupState({ animate: true, direction: 1, fromIndex: fromIdx });
      });
    }

    if (initialItems.length > 0) {
      initialItems.forEach((itemData) => {
        addGroupItem({ animate: false, data: itemData });
      });
      currentIndex = 0;
      refreshGroupState();
    } else {
      addGroupItem({ animate: false });
    }
  }

  // ---------- 計算フィールドのリアルタイム更新 ----------

  function collectFormData(form) {
    const data = {};
    if (!form) return data;
    const elements = form.querySelectorAll("input[name], select[name], textarea[name]");
    elements.forEach((el) => {
      const name = el.name;
      if (!name) return;
      if (el.type === "checkbox") {
        if (!el.checked) return;
      }
      const parts = name.split(".");
      let cursor = data;
      for (let i = 0; i < parts.length; i++) {
        const part = parts[i];
        const isLast = i === parts.length - 1;
        const nextPart = parts[i + 1];
        const nextIsIndex = nextPart !== undefined && /^\d+$/.test(nextPart);
        if (isLast) {
          if (part in cursor) {
            if (!Array.isArray(cursor[part])) {
              cursor[part] = [cursor[part]];
            }
            cursor[part].push(el.value);
          } else {
            cursor[part] = el.value;
          }
        } else {
          if (nextIsIndex) {
            if (!(part in cursor)) cursor[part] = [];
            cursor = cursor[part];
          } else if (/^\d+$/.test(part)) {
            const idx = parseInt(part, 10);
            while (cursor.length <= idx) cursor.push({});
            if (typeof cursor[idx] !== "object" || cursor[idx] === null) cursor[idx] = {};
            cursor = cursor[idx];
          } else {
            if (!(part in cursor) || typeof cursor[part] !== "object" || cursor[part] === null) {
              cursor[part] = {};
            }
            cursor = cursor[part];
          }
        }
      }
    });
    return data;
  }

  function resolveValue(data, dottedKey) {
    const parts = dottedKey.split(".");
    let current = data;
    for (let i = 0; i < parts.length; i++) {
      if (current == null) return undefined;
      if (Array.isArray(current)) {
        const remaining = parts.slice(i);
        const results = [];
        for (const item of current) {
          let sub = item;
          for (const subPart of remaining) {
            if (sub != null && typeof sub === "object" && !Array.isArray(sub)) {
              sub = sub[subPart];
            } else {
              sub = undefined;
              break;
            }
          }
          if (sub !== undefined) results.push(sub);
        }
        return results.length > 0 ? results : undefined;
      }
      if (typeof current === "object") {
        current = current[parts[i]];
      } else {
        return undefined;
      }
    }
    return current;
  }

  function collectNumericValues(value) {
    if (value == null) return [];
    if (Array.isArray(value)) {
      const result = [];
      for (const item of value) {
        const n = parseFloat(item);
        if (!isNaN(n)) result.push(n);
      }
      return result;
    }
    const n = parseFloat(value);
    return isNaN(n) ? [] : [n];
  }

  function applyAggregate(funcName, values) {
    if (funcName === "count") return values.length;
    if (values.length === 0) return 0;
    if (funcName === "sum") return values.reduce((a, b) => a + b, 0);
    if (funcName === "avg") return values.reduce((a, b) => a + b, 0) / values.length;
    if (funcName === "max") return Math.max(...values);
    if (funcName === "min") return Math.min(...values);
    return 0;
  }

  function evaluateFormulaClient(formula, data) {
    if (!formula) return null;
    let expr = formula.replace(/(sum|avg|count|max|min)\(\{([A-Za-z][A-Za-z0-9_.]*)\}\)/g, (_, func, ref) => {
      const value = resolveValue(data, ref);
      const nums = collectNumericValues(value);
      return String(applyAggregate(func, nums));
    });
    expr = expr.replace(/\{([A-Za-z][A-Za-z0-9_.]*)\}/g, (_, ref) => {
      const value = resolveValue(data, ref);
      if (value == null) return "0";
      const n = parseFloat(value);
      return isNaN(n) ? "0" : String(n);
    });
    if (!/^[\d\s+\-*/%().eE]+$/.test(expr)) return null;
    try {
      const result = Function('"use strict"; return (' + expr + ')')();
      if (typeof result === "number" && isFinite(result)) return result;
      return null;
    } catch {
      return null;
    }
  }

  function updateCalculatedFields() {
    const form = document.querySelector("form[method='post'][enctype='multipart/form-data']");
    if (!form) return;
    const data = collectFormData(form);
    const displays = form.querySelectorAll("[data-role='calculated-display']");
    displays.forEach((el) => {
      const formula = el.dataset.formula;
      if (!formula) return;
      const result = evaluateFormulaClient(formula, data);
      const hiddenInput = el.parentElement.querySelector("[data-role='calculated-value']");
      if (result !== null) {
        const formatted = Number.isInteger(result) ? String(result) : result.toFixed(10).replace(/\.?0+$/, "");
        el.textContent = formatted;
        el.classList.remove("text-slate-400");
        el.classList.add("text-slate-700", "font-medium");
        if (hiddenInput) hiddenInput.value = result;
      } else {
        el.innerHTML = '<span class="text-slate-400">自動計算</span>';
        el.classList.remove("font-medium");
        if (hiddenInput) hiddenInput.value = "";
      }
    });
  }

  let calcTimer = 0;
  function scheduleCalcUpdate() {
    if (calcTimer) window.clearTimeout(calcTimer);
    calcTimer = window.setTimeout(updateCalculatedFields, 120);
  }

  (function initCalculatedFieldWatcher() {
    const form = document.querySelector("form[method='post'][enctype='multipart/form-data']");
    if (!form) return;
    form.addEventListener("input", scheduleCalcUpdate);
    form.addEventListener("change", scheduleCalcUpdate);
    updateCalculatedFields();
  })();

  preventSubmitOnEnter(document.querySelector("form[method='post'][enctype='multipart/form-data']"));
  document.querySelectorAll("input[data-picker]").forEach(initPicker);
  document.querySelectorAll(".array-field").forEach(initArrayFieldBlock);
  document.querySelectorAll(".array-group").forEach(initArrayGroupBlock);
  initChoiceSelectSearch(document);
  initMasterReference(document);
