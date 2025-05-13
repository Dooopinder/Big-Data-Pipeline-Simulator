import streamlit as st
import networkx as nx
import matplotlib.pyplot as plt
import json
from collections import defaultdict

# === DAG builder ===
class PipelineDAG:
    def __init__(self):
        self.graph = nx.DiGraph()

    def add_node(self, node_id, node_type):
        self.graph.add_node(node_id, type=node_type)

    def add_edge(self, from_node, to_node):
        self.graph.add_edge(from_node, to_node)

    def draw(self, highlight=None):
        fig, ax = plt.subplots(figsize=(8, 6))
        try:
            pos = nx.nx_pydot.graphviz_layout(self.graph, prog="dot")
        except:
            pos = nx.spring_layout(self.graph)

        labels = nx.get_node_attributes(self.graph, 'type')
        custom_labels = {n: f"{n}\n({t})" for n, t in labels.items()}

        node_colors = ['orange' if n == highlight else 'skyblue' for n in self.graph.nodes()]
        nx.draw(self.graph, pos, with_labels=True, labels=custom_labels,
                node_color=node_colors, node_size=2000, font_size=10,
                font_weight='bold', edgecolors='black', linewidths=1.5,
                arrows=True, arrowsize=20, ax=ax)

        plt.title("Big Data Pipeline DAG", fontsize=14)
        st.pyplot(fig)

# === Spark-like transformation simulator ===
class SparkSim:
    def __init__(self, data):
        self.original = data
        self.data = data.copy()
        self.stage = 0
        self.logs = []

    def map(self):
        self.logs.append("→ map: Multiplied each value by 2")
        self.data = [(k, v * 2) for k, v in self.data]
        self.stage += 1

    def filter(self):
        self.logs.append("→ filter: Removed entries where key == 'banana'")
        self.data = [item for item in self.data if item[0] != "banana"]
        self.stage += 1

    def reduceByKey(self):
        self.logs.append("→ reduceByKey: Summed values grouped by key")
        grouped = defaultdict(list)
        for k, v in self.data:
            grouped[k].append(v)
        self.data = [(k, sum(vs)) for k, vs in grouped.items()]
        self.stage += 1

    def reset(self):
        self.data = self.original.copy()
        self.stage = 0
        self.logs.clear()

# === Streamlit App ===
st.set_page_config(page_title="Big Data Pipeline Simulator", layout="centered")
st.title("🧠 Big Data Pipeline Simulator")

# === JSON Upload ===
uploaded_file = st.file_uploader("📁 Upload a custom DAG JSON file", type=["json"])

pipeline = None  # always define upfront

if uploaded_file:
    try:
        pipeline = json.load(uploaded_file)
        st.success("Custom DAG loaded!")
    except Exception as e:
        st.error(f"Invalid JSON file: {e}")
        pipeline = None

# Use default if upload fails or not given
if not pipeline:
    pipeline = {
        "nodes": [
            {"id": "read", "type": "source"},
            {"id": "map1", "type": "transformation"},
            {"id": "filter1", "type": "transformation"},
            {"id": "reduce1", "type": "shuffle"},
            {"id": "output", "type": "sink"}
        ],
        "edges": [
            ["read", "map1"],
            ["map1", "filter1"],
            ["filter1", "reduce1"],
            ["reduce1", "output"]
        ]
    }

# === Build DAG from JSON ===
dag = PipelineDAG()
for node in pipeline["nodes"]:
    dag.add_node(node["id"], node["type"])
for edge in pipeline["edges"]:
    dag.add_edge(*edge)

# === Data Simulation ===
if "sim" not in st.session_state:
    st.session_state.sim = SparkSim([
        ("apple", 1),
        ("banana", 1),
        ("apple", 1),
        ("carrot", 1),
    ])

sim = st.session_state.sim

# === DAG Visualization ===
st.subheader("📊 DAG Visualization")
stage_nodes = [n["id"] for n in pipeline["nodes"] if "map" in n["id"] or "filter" in n["id"] or "reduce" in n["id"]]
current_stage = stage_nodes[sim.stage] if sim.stage < len(stage_nodes) else None
dag.draw(highlight=current_stage)

# === Pipeline Controls ===
st.subheader("⚙️ Pipeline Controls")
col1, col2, col3 = st.columns(3)
if col1.button("▶ Next Stage"):
    if sim.stage == 0:
        sim.map()
    elif sim.stage == 1:
        sim.filter()
    elif sim.stage == 2:
        sim.reduceByKey()
if col2.button("🔁 Reset"):
    sim.reset()

# === Logs and Output ===
st.subheader("🧾 Transformation Log")
if sim.logs:
    st.code("\n".join(sim.logs), language='text')
else:
    st.info("No stages executed yet.")

st.subheader("📦 Output Data")
st.json(sim.data)

# === Metrics ===
if sim.stage >= 3:
    st.subheader("📈 Evaluation Metrics")
    metric = st.selectbox("Select Metric", ["Total Unique Keys", "Max Value"])
    if metric == "Total Unique Keys":
        st.metric("Unique Keys", len(sim.data))
    elif metric == "Max Value":
        st.metric("Max Value", max([v for (_, v) in sim.data], default=0))

# === External Map Link ===
st.markdown("🔗 [Test map() at Python Tutor](https://pythontutor.com/visualize.html#code=map%28lambda%20x%3A%20x*2,%20%5B1,%202,%203%5D%29)")
