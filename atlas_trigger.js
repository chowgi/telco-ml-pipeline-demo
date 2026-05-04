// MongoDB Atlas Trigger Function
// Triggered when a new document is added to 'incoming_network_data'
exports = async function(changeEvent) {
    try {
      // Connect to the database
      let db;
      try {
        db = context.services.get("DemoTriggers").db("ods_demo_db");
      } catch (serviceError) {
        try {
          db = context.services.get("mongodb-atlas").db("ods_demo_db");
        } catch (altError) {
          try {
            db = context.services.get("mongodb").db("ods_demo_db");
          } catch (finalError) {
            console.error("All connection attempts failed:", {
              "DemoTriggers": serviceError.message,
              "mongodb-atlas": altError.message,
              "mongodb": finalError.message
            });
            throw new Error("Cannot connect to MongoDB service. Please check service configuration.");
          }
        }
      }
  
      const incomingCollection = db.collection("incoming_network_data");
      const resultsCollection = db.collection("network_health_predictions");
  
      // Get the newly inserted document
      const newDocument = changeEvent.fullDocument;
  
      if (!newDocument) {
        console.log("No document found in change event");
        return;
      }
  
      console.log("Processing new network data:", newDocument._id);
  
      // Validate required network health features
      const requiredFeatures = [
        'signal_strength_dbm',
        'throughput_mbps', 
        'latency_ms',
        'call_drop_rate_percent',
        'packet_loss_percent',
        'jitter_ms'
      ];
      
      const missingFeatures = requiredFeatures.filter(feature => 
        newDocument[feature] === undefined || newDocument[feature] === null
      );
      
      if (missingFeatures.length > 0) {
        console.error("Missing required features:", missingFeatures);
        return;
      }
  
      // Prepare MLflow API request payload for network health classifier
      const mlflowPayload = {
        dataframe_records: [
          {
            signal_strength_dbm: newDocument.signal_strength_dbm,
            throughput_mbps: newDocument.throughput_mbps,
            latency_ms: newDocument.latency_ms,
            call_drop_rate_percent: newDocument.call_drop_rate_percent,
            packet_loss_percent: newDocument.packet_loss_percent,
            jitter_ms: newDocument.jitter_ms
          }
        ]
      };
  
      // Call the MLflow model server (network health classifier on port 5003)
      const response = await context.http.post({
        url: "http://ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com:5003/invocations",
        headers: {
          "Content-Type": ["application/json"]
        },
        body: JSON.stringify(mlflowPayload)
      });
  
      if (response.statusCode === 200) {
        // Parse the API response
        const result = JSON.parse(response.body.text());
        console.log("Successfully processed network data:", newDocument._id, "Model response:", result);
  
        // Map prediction to human-readable labels
        const predictionLabels = {
          0: 'excellent',
          1: 'good', 
          2: 'poor'
        };
        
        const prediction = result.predictions ? result.predictions[0] : result[0];
        const healthScore = predictionLabels[prediction] || 'unknown';
  
        // Store result in 'network_health_predictions'
        const resultDoc = {
          network_data_id: newDocument._id,
          timestamp: new Date(),
          input_features: {
            signal_strength_dbm: newDocument.signal_strength_dbm,
            throughput_mbps: newDocument.throughput_mbps,
            latency_ms: newDocument.latency_ms,
            call_drop_rate_percent: newDocument.call_drop_rate_percent,
            packet_loss_percent: newDocument.packet_loss_percent,
            jitter_ms: newDocument.jitter_ms
          },
          prediction: {
            encoded: prediction,
            label: healthScore,
            confidence: "N/A" // MLflow doesn't return probabilities by default
          },
          metadata: {
            imsi: newDocument.imsi || null,
            customer_id: newDocument.customer_id || null,
            region: newDocument.region || null,
            device_type: newDocument.device_type || null,
            cell_technology: newDocument.cell_technology || null,
            test_id: newDocument.test_id || null,
            test_scenario: newDocument.test_scenario || null
          }
        };
  
        const insertResult = await resultsCollection.insertOne(resultDoc);
        console.log("Network health prediction stored in MongoDB with ID:", insertResult.insertedId);
  
      } else {
        console.error("API call failed with status:", response.statusCode);
        console.error("Response body:", response.body.text());
        console.error("API call failed - no prediction stored");
      }
    } catch (error) {
      console.error("Error processing network data:", error);
      console.error("Error processing network data - no prediction stored");
    }
  };
