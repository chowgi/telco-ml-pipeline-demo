// MongoDB Atlas Database Trigger
// Fires on insert to 'windowed_network_metrics' collection
// Calls MLflow inference endpoint and stores prediction
exports = async function(changeEvent) {
  try {
    let db;
    try {
      db = context.services.get("DemoTriggers").db("ods_demo_db");
    } catch (e1) {
      try {
        db = context.services.get("mongodb-atlas").db("ods_demo_db");
      } catch (e2) {
        db = context.services.get("mongodb").db("ods_demo_db");
      }
    }

    const resultsCollection = db.collection("network_health_predictions");
    const doc = changeEvent.fullDocument;

    if (!doc) {
      console.log("No document in change event");
      return;
    }

    // Extract avg values from windowed aggregates
    const signal = typeof doc.signal_strength_dbm === 'object'
      ? doc.signal_strength_dbm.avg : doc.signal_strength_dbm;
    const throughput = typeof doc.throughput_mbps === 'object'
      ? doc.throughput_mbps.avg : doc.throughput_mbps;
    const latency = typeof doc.latency_ms === 'object'
      ? doc.latency_ms.avg : doc.latency_ms;
    const dropRate = typeof doc.call_drop_rate_percent === 'object'
      ? doc.call_drop_rate_percent.avg : doc.call_drop_rate_percent;
    const packetLoss = typeof doc.packet_loss_percent === 'object'
      ? doc.packet_loss_percent.avg : doc.packet_loss_percent;
    const jitter = typeof doc.jitter_ms === 'object'
      ? doc.jitter_ms.avg : doc.jitter_ms;

    if ([signal, throughput, latency, dropRate, packetLoss, jitter].some(v => v === undefined || v === null)) {
      console.error("Missing required features in windowed doc:", doc._id);
      return;
    }

    const mlflowPayload = {
      dataframe_records: [{
        signal_strength_dbm: signal,
        throughput_mbps: throughput,
        latency_ms: latency,
        call_drop_rate_percent: dropRate,
        packet_loss_percent: packetLoss,
        jitter_ms: jitter
      }]
    };

    // MLFLOW_ENDPOINT should be set as an Atlas App Services Value/Secret
    const mlflowEndpoint = context.values.get("MLFLOW_ENDPOINT") ||
      "http://<mlflow-ec2-public-ip>:5003/invocations";

    const response = await context.http.post({
      url: mlflowEndpoint,
      headers: { "Content-Type": ["application/json"] },
      body: JSON.stringify(mlflowPayload)
    });

    if (response.statusCode === 200) {
      const result = JSON.parse(response.body.text());
      const predictionLabels = { 0: "excellent", 1: "good", 2: "poor" };
      const prediction = result.predictions ? result.predictions[0] : result[0];
      const healthLabel = predictionLabels[prediction] || "unknown";

      const predictionDoc = {
        windowed_metrics_id: doc._id,
        timestamp: new Date(),
        window_end: doc.window_end,
        cell_id: doc.cell_id,
        region: doc.region,
        input_features: {
          signal_strength_dbm: signal,
          throughput_mbps: throughput,
          latency_ms: latency,
          call_drop_rate_percent: dropRate,
          packet_loss_percent: packetLoss,
          jitter_ms: jitter
        },
        prediction: {
          encoded: prediction,
          label: healthLabel
        },
        event_count: doc.event_count,
        anomaly_event_count: doc.anomaly_event_count
      };

      await resultsCollection.insertOne(predictionDoc);
      console.log(`Prediction stored: cell=${doc.cell_id} health=${healthLabel}`);
    } else {
      console.error("MLflow call failed:", response.statusCode, response.body.text());
    }
  } catch (error) {
    console.error("Trigger error:", error);
  }
};
