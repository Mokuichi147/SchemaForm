// 集計ページの描画と操作。
//
// 集計そのものはサーバー側で終わっており、ここでやることは3つだけ。
//   1. サーバーが埋め込んだ数値を Chart.js で描く
//   2. グラフをクリックしたら「絞り込み条件」をURLに足して読み込み直す
//   3. 件数の推移の粒度バケットを、閲覧者のローカル時刻で組み立てる
// 絞り込みの判定はサーバー側と共通なので、グラフの件数と絞り込み後の件数は必ず一致する。
(() => {
  const PALETTE = [
    "#2563eb", "#16a34a", "#dc2626", "#d97706", "#7c3aed", "#0891b2",
    "#db2777", "#65a30d", "#ea580c", "#4f46e5", "#0d9488", "#9333ea",
  ];

  const root = document.getElementById("aggregate-root");
  if (!root) return;
  const DRILL_PARAM = root.dataset.drillParam || "drill";
  const CORRECT_PARAM = root.dataset.correctParam || "correct";
  const FORM_ID = root.dataset.formId || "";
  const STORAGE_KEY = "sf-correct:" + FORM_ID;

  function parseJson(text, fallback) {
    try {
      return JSON.parse(text);
    } catch (e) {
      return fallback;
    }
  }

  // ---- URL の組み立て -------------------------------------------------------

  function currentDrill() {
    return parseJson(root.dataset.drill || "[]", []);
  }

  function sameCondition(a, b) {
    if (a.op !== b.op) return false;
    // 送信日時・正答数はページ全体で1つだけ。項目の条件は項目ごとに1つだけ。
    return a.op === "ts" || a.op === "score" ? true : a.k === b.k;
  }

  function go(params) {
    const search = new URLSearchParams(window.location.search);
    Object.entries(params).forEach(([key, value]) => {
      if (value === null || value === "") search.delete(key);
      else search.set(key, value);
    });
    search.delete("page");
    window.location.search = search.toString();
  }

  function applyDrill(condition) {
    if (!condition) return;
    const next = currentDrill().filter((item) => !sameCondition(item, condition));
    next.push(condition);
    go({ [DRILL_PARAM]: JSON.stringify(next) });
  }

  // ---- グラフ ---------------------------------------------------------------

  function buildConfig(spec) {
    const isLine = spec.type === "line";
    const isPie = spec.type === "pie";
    // ラベルごとに元の並び順で色を固定し、棒・円で同じ項目が同色になるようにする。
    const colorByLabel = {};
    spec.labels.forEach((label, i) => {
      colorByLabel[label] = PALETTE[i % PALETTE.length];
    });

    let rows = spec.labels.map((label, i) => ({
      label,
      value: spec.counts[i],
      drill: (spec.drills || [])[i] || null,
    }));
    // 円グラフは割合の多い順に並べ替える。
    if (isPie) rows = rows.slice().sort((a, b) => (b.value || 0) - (a.value || 0));

    const colors = rows.map((row) => colorByLabel[row.label]);
    const fill = isLine ? "rgba(37, 99, 235, 0.2)" : spec.mono ? PALETTE[0] : colors;
    const stroke = isLine ? "#2563eb" : spec.mono ? PALETTE[0] : colors;

    return {
      type: spec.type,
      data: {
        labels: rows.map((row) => row.label),
        datasets: [{
          label: spec.label || "",
          data: rows.map((row) => row.value),
          backgroundColor: fill,
          borderColor: stroke,
          borderWidth: isLine ? 2 : 1,
          fill: isLine,
          tension: isLine ? 0.2 : 0,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: isPie ? undefined : { mode: "index", intersect: false },
        onClick: (evt, elements) => {
          if (elements.length) applyDrill(rows[elements[0].index].drill);
        },
        onHover: (evt, elements) => {
          const clickable = elements.length && rows[elements[0].index].drill;
          evt.native.target.style.cursor = clickable ? "pointer" : "default";
        },
        plugins: {
          legend: { display: isPie },
          tooltip: {
            callbacks: {
              label: (ctx) => {
                const value = typeof ctx.raw === "number" ? ctx.raw : ctx.parsed.y ?? ctx.parsed;
                const data = ctx.dataset.data || [];
                const total = data.reduce((sum, v) => sum + (Number(v) || 0), 0);
                const pct = total > 0 ? ((value / total) * 100).toFixed(1) : "0.0";
                return spec.suffix === false
                  ? String(value)
                  : value + "件 (" + pct + "%)";
              },
            },
          },
        },
        scales: isPie ? {} : { y: { beginAtZero: true, ticks: { precision: 0 } } },
      },
    };
  }

  const charts = new WeakMap();

  function render(canvas, spec) {
    const existing = charts.get(canvas);
    if (existing) existing.destroy();
    const chart = new Chart(canvas, buildConfig(spec));
    charts.set(canvas, chart);
    return chart;
  }

  function setupSimpleCharts() {
    document.querySelectorAll("canvas[data-chart]").forEach((canvas) => {
      const spec = parseJson(canvas.dataset.chart, null);
      if (!spec) return;
      render(canvas, spec);
      // 棒／円の切り替えは同じカード内のボタンから。
      const card = canvas.closest("[data-card]");
      card?.querySelectorAll("[data-chart-type]").forEach((button) => {
        button.addEventListener("click", () => {
          spec.type = button.dataset.chartType;
          render(canvas, spec);
          card.querySelectorAll("[data-chart-type]").forEach((other) => {
            other.classList.toggle("bg-slate-900", other === button);
            other.classList.toggle("text-white", other === button);
          });
        });
      });
    });
  }

  // ---- クロス集計（指標と項目を切り替える） --------------------------------

  const METRIC_LABELS = {
    count: "件数", sum: "合計", avg: "平均", median: "中央値", max: "最大", min: "最小",
  };

  function setupCrosstabs() {
    document.querySelectorAll("[data-crosstab-card]").forEach((card) => {
      const wraps = Array.from(card.querySelectorAll("[data-crosstab-wrap]"));
      const groupSelect = card.querySelector("[data-crosstab-group]");
      const metricSelect = card.querySelector("[data-crosstab-metric]");
      let type = "bar";

      const visibleCanvas = () => {
        const wrap = wraps.find((w) => !w.classList.contains("hidden"));
        return wrap ? wrap.querySelector("canvas") : null;
      };

      // 表示中のグラフだけ描く。hidden な canvas は Chart.js のサイズが 0 になるため。
      const draw = () => {
        const canvas = visibleCanvas();
        if (!canvas) return;
        const data = parseJson(canvas.dataset.crosstab, null);
        if (!data) return;
        const metric = metricSelect ? metricSelect.value : "sum";
        render(canvas, {
          type,
          labels: data.labels,
          counts: data.metrics[metric],
          drills: data.drills,
          label: METRIC_LABELS[metric] || metric,
          suffix: false,
        });
      };

      groupSelect?.addEventListener("change", () => {
        const index = parseInt(groupSelect.value, 10);
        wraps.forEach((wrap, i) => wrap.classList.toggle("hidden", i !== index));
        draw();
      });
      metricSelect?.addEventListener("change", draw);
      card.querySelectorAll("[data-crosstab-type]").forEach((button) => {
        button.addEventListener("click", () => {
          type = button.dataset.crosstabType;
          card.querySelectorAll("[data-crosstab-type]").forEach((other) => {
            other.classList.toggle("bg-slate-900", other === button);
            other.classList.toggle("text-white", other === button);
          });
          draw();
        });
      });
      draw();
    });
  }

  // ---- 件数の推移（粒度は閲覧者のローカル時刻で組み立てる） ----------------

  const TS_UNITS = ["second", "minute", "hour", "day", "week", "month", "year"];
  // バーが多すぎる粒度は不可。充填率(データの入る区間の割合)がこれ未満なら
  // 0件区間が多すぎるとみなし、1段階広い粒度を選ぶ。
  const TS_MAX_BUCKETS = 60;
  const TS_MIN_FILL = 0.34;
  const TS_UNIT_SECONDS = { second: 1, minute: 60, hour: 3600, day: 86400, week: 604800 };

  function tsTruncate(date, unit) {
    const Y = date.getFullYear(), Mo = date.getMonth(), D = date.getDate();
    const H = date.getHours(), Mi = date.getMinutes(), S = date.getSeconds();
    if (unit === "second") return new Date(Y, Mo, D, H, Mi, S);
    if (unit === "minute") return new Date(Y, Mo, D, H, Mi, 0);
    if (unit === "hour") return new Date(Y, Mo, D, H, 0, 0);
    if (unit === "day") return new Date(Y, Mo, D, 0, 0, 0);
    if (unit === "week") {
      const d = new Date(Y, Mo, D);
      d.setDate(d.getDate() - ((d.getDay() + 6) % 7));
      return d;
    }
    if (unit === "month") return new Date(Y, Mo, 1);
    return new Date(Y, 0, 1);
  }

  function tsNext(date, unit) {
    const d = new Date(date.getTime());
    if (unit === "second") d.setSeconds(d.getSeconds() + 1);
    else if (unit === "minute") d.setMinutes(d.getMinutes() + 1);
    else if (unit === "hour") d.setHours(d.getHours() + 1);
    else if (unit === "day") d.setDate(d.getDate() + 1);
    else if (unit === "week") d.setDate(d.getDate() + 7);
    else if (unit === "month") d.setMonth(d.getMonth() + 1);
    else d.setFullYear(d.getFullYear() + 1);
    return d;
  }

  function tsLabel(date, unit) {
    const pad = (n) => String(n).padStart(2, "0");
    const Y = date.getFullYear(), Mo = pad(date.getMonth() + 1), D = pad(date.getDate());
    const H = pad(date.getHours()), Mi = pad(date.getMinutes()), S = pad(date.getSeconds());
    if (unit === "second") return `${Y}-${Mo}-${D} ${H}:${Mi}:${S}`;
    if (unit === "minute") return `${Y}-${Mo}-${D} ${H}:${Mi}`;
    if (unit === "hour") return `${Y}-${Mo}-${D} ${H}:00`;
    if (unit === "day" || unit === "week") return `${Y}-${Mo}-${D}`;
    if (unit === "month") return `${Y}-${Mo}`;
    return String(Y);
  }

  function tsBucketCount(minD, maxD, unit) {
    const lo = tsTruncate(minD, unit), hi = tsTruncate(maxD, unit);
    if (unit === "year") return hi.getFullYear() - lo.getFullYear() + 1;
    if (unit === "month") {
      return (hi.getFullYear() - lo.getFullYear()) * 12 + (hi.getMonth() - lo.getMonth()) + 1;
    }
    return Math.floor((hi - lo) / (TS_UNIT_SECONDS[unit] * 1000)) + 1;
  }

  function tsAutoUnit(dates) {
    // なるべく広い粒度を優先。細→粗に見て、バー数が上限以下で、かつ空区間が
    // 多すぎない(充填率が十分な)最も細かい粒度を選ぶ。
    const filled = (unit) => new Set(dates.map((d) => tsLabel(tsTruncate(d, unit), unit))).size;
    let chosen = "year";
    for (const unit of TS_UNITS) {
      const count = tsBucketCount(dates[0], dates[dates.length - 1], unit);
      if (count > TS_MAX_BUCKETS) continue;
      chosen = unit;
      if (count <= 3 || filled(unit) / count >= TS_MIN_FILL) return unit;
    }
    return chosen;
  }

  function setupTimeseries() {
    const section = document.getElementById("timeseries-card");
    if (!section) return;
    const canvas = section.querySelector("canvas");
    const unitSelect = section.querySelector("[data-ts-unit]");
    const dates = parseJson(section.dataset.timestamps || "[]", [])
      .map((text) => new Date(text))
      .filter((date) => !Number.isNaN(date.getTime()))
      .sort((a, b) => a - b);
    if (!canvas || !dates.length) return;

    const draw = () => {
      const requested = unitSelect ? unitSelect.value : "auto";
      const unit = TS_UNITS.includes(requested) ? requested : tsAutoUnit(dates);
      const counter = {};
      dates.forEach((date) => {
        const key = tsLabel(tsTruncate(date, unit), unit);
        counter[key] = (counter[key] || 0) + 1;
      });
      const labels = [], counts = [], drills = [];
      let cursor = tsTruncate(dates[0], unit);
      const end = tsTruncate(dates[dates.length - 1], unit);
      let guard = 0;
      while (cursor <= end && guard < 2000) {
        const key = tsLabel(cursor, unit);
        const next = tsNext(cursor, unit);
        labels.push(key);
        counts.push(counter[key] || 0);
        // 区間は [開始, 次の開始) の絶対時刻で送り、サーバー側で同じ区間を再現する。
        // t は絞り込み中の表示に使う見出し（ローカル時刻）。
        drills.push({ op: "ts", lo: cursor.toISOString(), hi: next.toISOString(), t: key });
        cursor = next;
        guard += 1;
      }
      render(canvas, { type: "line", labels, counts, drills, label: "件数" });
    };

    unitSelect?.addEventListener("change", draw);
    draw();
  }

  // ---- テスト採点の正解入力 -------------------------------------------------

  function setupScoring() {
    const panel = document.getElementById("scoring-panel");
    if (!panel) return;

    function readCorrectMap() {
      const map = {};
      panel.querySelectorAll("[data-question]").forEach((row) => {
        const key = row.dataset.question;
        if (row.dataset.array === "1") {
          const list = parseJson(row.dataset.selected || "[]", []);
          if (list.length) {
            map[key] = {
              mode: "array",
              list,
              ordered: !!row.querySelector("[data-ordered]")?.checked,
            };
          }
        } else {
          const value = row.querySelector("select")?.value || "";
          if (value) map[key] = { mode: "single", value };
        }
      });
      return map;
    }

    // 複数選択の設問は、入力ページと同じバッジ式UIで正解を並べる。
    panel.querySelectorAll('[data-question][data-array="1"]').forEach((row) => {
      const select = row.querySelector("[data-add-select]");
      const badges = row.querySelector("[data-badges]");
      const unique = row.dataset.unique === "1";
      let selected = parseJson(row.dataset.selected || "[]", []);

      const sync = () => {
        row.dataset.selected = JSON.stringify(selected);
        badges.replaceChildren();
        selected.forEach((value, index) => {
          const chip = document.createElement("button");
          chip.type = "button";
          chip.className =
            "inline-flex max-w-full items-center gap-1 rounded-full border border-slate-300 bg-white px-2.5 py-1 text-xs text-slate-700 hover:border-rose-300 hover:text-rose-700";
          chip.title = "クリックして削除";
          chip.textContent = value + " ×";
          chip.addEventListener("click", () => {
            selected.splice(index, 1);
            sync();
          });
          badges.appendChild(chip);
        });
      };

      row.querySelector("[data-add]")?.addEventListener("click", () => {
        const value = select.value;
        if (!value) return;
        if (unique && selected.includes(value)) return;
        selected.push(value);
        select.value = "";
        sync();
      });
      sync();
    });

    const apply = (map) => {
      const encoded = Object.keys(map).length ? JSON.stringify(map) : null;
      try {
        if (encoded) localStorage.setItem(STORAGE_KEY, encoded);
        else localStorage.removeItem(STORAGE_KEY);
      } catch (e) {}
      go({ [CORRECT_PARAM]: encoded, [DRILL_PARAM]: null });
    };

    panel.querySelector("[data-apply-correct]")?.addEventListener("click", () => {
      apply(readCorrectMap());
    });
    panel.querySelector("[data-clear-correct]")?.addEventListener("click", () => apply({}));

    // 前回この端末で設定した正解を呼び戻せるようにする（URLが正、これは控え）。
    const restore = panel.querySelector("[data-restore-correct]");
    if (restore) {
      let saved = null;
      try {
        saved = localStorage.getItem(STORAGE_KEY);
      } catch (e) {}
      if (saved && saved !== root.dataset.correct) {
        restore.classList.remove("hidden");
        restore.addEventListener("click", () => go({ [CORRECT_PARAM]: saved }));
      }
    }
  }

  // ---- 起動 ---------------------------------------------------------------

  document.addEventListener("DOMContentLoaded", () => {
    setupScoring();
    if (typeof Chart === "undefined") {
      // Chart.js を読めなかったときは、白紙のまま放置せず理由を出す。
      document.querySelectorAll("[data-chart-area]").forEach((area) => {
        area.innerHTML =
          '<p class="flex h-full items-center justify-center text-sm text-slate-400">' +
          "グラフの表示に必要なライブラリを読み込めませんでした。</p>";
      });
      return;
    }
    setupSimpleCharts();
    setupCrosstabs();
    setupTimeseries();
  });
})();
