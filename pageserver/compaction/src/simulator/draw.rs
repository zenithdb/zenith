use super::Key;
use anyhow::Result;
use std::cmp::Ordering;
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::Write,
    ops::Range,
};
use svg_fmt::{rgb, BeginSvg, EndSvg, Fill, Stroke, Style};
use utils::lsn::Lsn;

// Map values to their compressed coordinate - the index the value
// would have in a sorted and deduplicated list of all values.
struct CoordinateMap<T: Ord + Copy> {
    map: BTreeMap<T, usize>,
    stretch: f32,
}

impl<T: Ord + Copy> CoordinateMap<T> {
    fn new(coords: Vec<T>, stretch: f32) -> Self {
        let set: BTreeSet<T> = coords.into_iter().collect();

        let mut map: BTreeMap<T, usize> = BTreeMap::new();
        for (i, e) in set.iter().enumerate() {
            map.insert(*e, i);
        }

        Self { map, stretch }
    }

    // This assumes that the map contains an exact point for this.
    // Use map_inexact for values inbetween
    fn map(&self, val: T) -> f32 {
        *self.map.get(&val).unwrap() as f32 * self.stretch
    }

    // the value is still assumed to be within the min/max bounds
    // (this is currently unused)
    fn _map_inexact(&self, val: T) -> f32 {
        let prev = *self.map.range(..=val).next().unwrap().1;
        let next = *self.map.range(val..).next().unwrap().1;

        // interpolate
        (prev as f32 + (next - prev) as f32) * self.stretch
    }

    fn max(&self) -> f32 {
        self.map.len() as f32 * self.stretch
    }
}

#[derive(PartialEq, Hash, Eq)]
pub enum LayerTraceOp {
    Flush,
    CreateDelta,
    CreateImage,
    Delete,
}

impl std::fmt::Display for LayerTraceOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let op_str = match self {
            LayerTraceOp::Flush => "flush",
            LayerTraceOp::CreateDelta => "create_delta",
            LayerTraceOp::CreateImage => "create_image",
            LayerTraceOp::Delete => "delete",
        };
        f.write_str(op_str)
    }
}

#[derive(PartialEq, Hash, Eq, Clone)]
pub struct LayerTraceFile {
    pub filename: String,
    pub key_range: Range<Key>,
    pub lsn_range: Range<Lsn>,
}

impl LayerTraceFile {
    fn is_image(&self) -> bool {
        self.lsn_range.end == self.lsn_range.start
    }
}

pub struct LayerTraceEvent {
    pub time_rel: u64,
    pub op: LayerTraceOp,
    pub file: LayerTraceFile,
}

pub fn draw_history<W: std::io::Write>(history: &[LayerTraceEvent], mut output: W) -> Result<()> {
    let mut files: Vec<LayerTraceFile> = Vec::new();

    for event in history {
        files.push(event.file.clone());
    }
    let last_time_rel = history.last().unwrap().time_rel;

    // Collect all coordinates
    let mut keys: Vec<Key> = vec![];
    let mut lsns: Vec<Lsn> = vec![];
    for f in files.iter() {
        keys.push(f.key_range.start);
        keys.push(f.key_range.end);
        lsns.push(f.lsn_range.start);
        lsns.push(f.lsn_range.end);
    }

    // Analyze
    let key_map = CoordinateMap::new(keys, 2.0);
    // Stretch out vertically for better visibility
    let lsn_map = CoordinateMap::new(lsns, 3.0);

    let mut svg = String::new();

    // Draw
    writeln!(
        svg,
        "{}",
        BeginSvg {
            w: key_map.max(),
            h: lsn_map.max(),
        }
    )?;
    let lsn_max = lsn_map.max();

    // Sort the files by LSN, but so that image layers go after all delta layers
    // The SVG is painted in the order the elements appear, and we want to draw
    // image layers on top of the delta layers if they overlap
    //
    // (This could also be implemented via z coordinates: image layers get one z
    // coord, delta layers get another z coord.)
    let mut files_sorted: Vec<LayerTraceFile> = files.into_iter().collect();
    files_sorted.sort_by(|a, b| {
        if a.is_image() && !b.is_image() {
            Ordering::Greater
        } else if !a.is_image() && b.is_image() {
            Ordering::Less
        } else {
            a.lsn_range.end.cmp(&b.lsn_range.end)
        }
    });

    writeln!(svg, "<!-- layers -->")?;
    let mut files_seen = HashSet::new();
    for f in files_sorted {
        if files_seen.contains(&f) {
            continue;
        }
        let key_start = key_map.map(f.key_range.start);
        let key_end = key_map.map(f.key_range.end);
        let key_diff = key_end - key_start;

        if key_start >= key_end {
            panic!("Invalid key range {}-{}", key_start, key_end);
        }

        let lsn_start = lsn_map.map(f.lsn_range.start);
        let lsn_end = lsn_map.map(f.lsn_range.end);

        // Fill in and thicken rectangle if it's an
        // image layer so that we can see it.
        let mut style = Style {
            fill: Fill::Color(rgb(0x80, 0x80, 0x80)),
            stroke: Stroke::Color(rgb(0, 0, 0), 0.5),
            opacity: 1.0,
            stroke_opacity: 1.0,
        };

        let y_start = lsn_max - lsn_start;
        let y_end = lsn_max - lsn_end;

        let x_margin = 0.25;
        let y_margin = 0.5;

        match f.lsn_range.start.cmp(&f.lsn_range.end) {
            Ordering::Less => {
                write!(
                    svg,
                    r#"    <rect id="layer_{}" x="{}" y="{}" width="{}" height="{}" ry="{}" style="{}">"#,
                    f.filename,
                    key_start + x_margin,
                    y_end + y_margin,
                    key_diff - x_margin * 2.0,
                    y_start - y_end - y_margin * 2.0,
                    1.0, // border_radius,
                    style,
                )?;
                write!(svg, "<title>{}</title>", f.filename)?;
                writeln!(svg, "</rect>")?;
            }
            Ordering::Equal => {
                //lsn_diff = 0.3;
                //lsn_offset = -lsn_diff / 2.0;
                //margin = 0.05;
                style.fill = Fill::Color(rgb(0x80, 0, 0x80));
                style.stroke = Stroke::Color(rgb(0x80, 0, 0x80), 3.0);
                write!(
                    svg,
                    r#"    <line id="layer_{}" x1="{}" y1="{}" x2="{}" y2="{}" style="{}">"#,
                    f.filename,
                    key_start + x_margin,
                    y_end,
                    key_end - x_margin,
                    y_end,
                    style,
                )?;
                write!(
                    svg,
                    "<title>{}<br>{} - {}</title>",
                    f.filename, lsn_end, y_end
                )?;
                writeln!(svg, "</line>")?;
            }
            Ordering::Greater => panic!("Invalid lsn range {}-{}", lsn_start, lsn_end),
        }
        files_seen.insert(f);
    }

    writeln!(svg, "{}", EndSvg)?;

    let mut layer_events_str = String::new();
    let mut first = true;
    for e in history {
        if !first {
            writeln!(layer_events_str, ",")?;
        }
        write!(
            layer_events_str,
            r#"  {{"time_rel": {}, "filename": "{}", "op": "{}"}}"#,
            e.time_rel, e.file.filename, e.op
        )?;
        first = false;
    }
    writeln!(layer_events_str)?;

    writeln!(
        output,
        r#"<!DOCTYPE html>
<html>
<head>
<style>
/* Keep the slider pinned at top */
.topbar {{
  display: block;
  overflow: hidden;
  background-color: lightgrey;
  position: fixed;
  top: 0;
  width: 100%;
/*  width: 500px; */
}}
.slidercontainer {{
  float: left;
  width: 50%;
  margin-right: 200px;
}}
.slider {{
  float: left;
  width: 100%;
}}
.legend {{
  width: 200px;
  float: right;
}}

/* Main content */
.main {{
  margin-top: 50px; /* Add a top margin to avoid content overlay */
}}
</style>
</head>

  <body onload="init()">
    <script type="text/javascript">

      var layer_events = [{layer_events_str}]

      let ticker;

      function init() {{
          for (let i = 0; i < layer_events.length; i++) {{
              var layer = document.getElementById("layer_" + layer_events[i].filename);
              layer.style.visibility = "hidden";
          }}
          last_layer_event = -1;
          moveSlider(last_slider_pos)
      }}

      function startAnimation() {{
          ticker = setInterval(animateStep, 100);
      }}
      function stopAnimation() {{
          clearInterval(ticker);
      }}

      function animateStep() {{
          if (last_layer_event < layer_events.length - 1) {{
              var slider = document.getElementById("time-slider");
              let prevPos = slider.value
              let nextEvent = last_layer_event + 1
              while (nextEvent <= layer_events.length - 1) {{
                  if (layer_events[nextEvent].time_rel > prevPos) {{
                      break;
                  }}
                  nextEvent += 1;
              }}
              let nextPos = layer_events[nextEvent].time_rel
              slider.value = nextPos
              moveSlider(nextPos)
          }}
      }}

      function redoLayerEvent(n, dir) {{
          var layer = document.getElementById("layer_" + layer_events[n].filename);
          switch (layer_events[n].op) {{
              case "flush":
                  layer.style.visibility = "visible";
                  break;
              case "create_delta":
                  layer.style.visibility = "visible";
                  break;
              case "create_image":
                  layer.style.visibility = "visible";
                  break;
              case "delete":
                  layer.style.visibility = "hidden";
                  break;
          }}
      }}
      function undoLayerEvent(n) {{
          var layer = document.getElementById("layer_" + layer_events[n].filename);
          switch (layer_events[n].op) {{
              case "flush":
                  layer.style.visibility = "hidden";
                  break;
              case "create_delta":
                  layer.style.visibility = "hidden";
                  break;
              case "create_image":
                  layer.style.visibility = "hidden";
                  break;
              case "delete":
                  layer.style.visibility = "visible";
                  break;
          }}
      }}

      var last_slider_pos = 0
      var last_layer_event = 0

      var moveSlider = function(new_pos) {{
          if (new_pos > last_slider_pos) {{
              while (last_layer_event < layer_events.length - 1) {{
                  if (layer_events[last_layer_event + 1].time_rel > new_pos) {{
                      break;
                  }}
                  last_layer_event += 1;
                  redoLayerEvent(last_layer_event)
              }}
          }}
          if (new_pos < last_slider_pos) {{
              while (last_layer_event >= 0) {{
                  if (layer_events[last_layer_event].time_rel <= new_pos) {{
                      break;
                  }}
                  undoLayerEvent(last_layer_event)
                  last_layer_event -= 1;
              }}
          }}
          last_slider_pos = new_pos;
          document.getElementById("debug_pos").textContent=new_pos;
          if (last_layer_event >= 0) {{
              document.getElementById("debug_layer_event").textContent=last_layer_event + " " + layer_events[last_layer_event].time_rel + " " + layer_events[last_layer_event].op;
          }} else {{
              document.getElementById("debug_layer_event").textContent="begin";
          }}
      }}
    </script>

    <div class="topbar">
      <div class="slidercontainer">
        <label for="time-slider">TIME</label>:
        <input id="time-slider" class="slider" type="range" min="0" max="{last_time_rel}" value="0" oninput="moveSlider(this.value)"><br>

        pos: <span id="debug_pos"></span><br>
        event: <span id="debug_layer_event"></span><br>
        gc: <span id="debug_gc_event"></span><br>
      </div>

      <button onclick="startAnimation()">Play</button>
      <button onclick="stopAnimation()">Stop</button>

      <svg class="legend">
        <rect x=5 y=0 width=20 height=20 style="fill:rgb(128,128,128);stroke:rgb(0,0,0);stroke-width:0.5;fill-opacity:1;stroke-opacity:1;"/>
        <line x1=5 y1=30 x2=25 y2=30 style="fill:rgb(128,0,128);stroke:rgb(128,0,128);stroke-width:3;fill-opacity:1;stroke-opacity:1;"/>
        <line x1=0 y1=40 x2=30 y2=40 style="fill:none;stroke:rgb(255,0,0);stroke-width:0.5;fill-opacity:1;stroke-opacity:1;"/>
      </svg>
    </div>

    <div class="main">
{svg}
    </div>
  </body>
</html>
"#
    )?;

    Ok(())
}
