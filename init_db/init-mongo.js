const dbName = "labdb";
const dbRef = db.getSiblingDB(dbName);

dbRef.createCollection("social_nodes");
dbRef.createCollection("social_edges");
dbRef.createCollection("graph_tasks");
dbRef.createCollection("graph_results");

dbRef.social_nodes.createIndex({ node_id: 1 }, { unique: true });
dbRef.social_nodes.createIndex({ community: 1 });
dbRef.social_edges.createIndex({ source: 1, target: 1 }, { unique: true });
dbRef.social_edges.createIndex({ source: 1 });
dbRef.social_edges.createIndex({ target: 1 });
dbRef.graph_tasks.createIndex({ status: 1, processing_until: 1, priority: -1, created_at: 1 });
dbRef.graph_tasks.createIndex({ worker_id: 1, updated_at: -1 });
dbRef.graph_results.createIndex({ task_id: 1 }, { unique: true });
dbRef.graph_results.createIndex({ root_node: 1 });

if (dbRef.social_nodes.countDocuments() === 0) {
  dbRef.social_nodes.insertMany([
    { node_id: "u1", community: "alpha" },
    { node_id: "u2", community: "alpha" },
    { node_id: "u3", community: "alpha" },
    { node_id: "u4", community: "beta" },
    { node_id: "u5", community: "beta" },
    { node_id: "u6", community: "beta" },
    { node_id: "u7", community: "gamma" },
    { node_id: "u8", community: "gamma" },
  ]);
}

if (dbRef.social_edges.countDocuments() === 0) {
  const undirectedEdges = [
    ["u1", "u2"],
    ["u1", "u3"],
    ["u2", "u3"],
    ["u3", "u4"],
    ["u4", "u5"],
    ["u4", "u6"],
    ["u5", "u6"],
    ["u6", "u7"],
    ["u7", "u8"],
  ];

  const docs = [];
  for (const [source, target] of undirectedEdges) {
    docs.push({ source, target, weight: 1 });
    docs.push({ source: target, target: source, weight: 1 });
  }

  dbRef.social_edges.insertMany(docs);
}

if (dbRef.graph_tasks.countDocuments() === 0) {
  const nodes = dbRef.social_nodes.find({}, { node_id: 1, community: 1 }).toArray();
  const tasks = [];
  const taskBatches = 4;

  for (let batch = 0; batch < taskBatches; batch += 1) {
    for (let i = 0; i < nodes.length; i += 1) {
      tasks.push({
        payload: {
          root_node: nodes[i].node_id,
          depth_limit: (batch + i) % 2 === 0 ? 2 : 3,
          analysis_type: "connectivity",
          community_hint: nodes[i].community,
          batch_id: batch + 1,
        },
        status: "pending",
        worker_id: null,
        processing_until: null,
        attempts: 0,
        priority: 1,
        result_ref: null,
        created_at: new Date(),
        updated_at: new Date(),
      });
    }
  }

  dbRef.graph_tasks.insertMany(tasks);
}
