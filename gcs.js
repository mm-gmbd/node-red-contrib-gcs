module.exports = function(RED) {
    "use strict";
    var fs = require('fs-extra');

    function GCSNode(n) {
      RED.nodes.createNode(this,n);
      if (this.credentials &&
          this.credentials.projectid && this.credentials.keyfilename) {

          var config = {
            projectId: this.credentials.projectid,
            keyFilename: this.credentials.keyfilename
          };

          this.GCS = require('@google-cloud/storage')(config);
      }
    }

    RED.nodes.registerType("gcs-config",GCSNode,{
        credentials: {
            projectid: { type:"text" },
            keyfilename: { type: "text" }
        }
    });

    function GCSFileUploadNode(n) {
        RED.nodes.createNode(this,n);
        this.gcsConfig = RED.nodes.getNode(n.gcs);
        this.localfilename = n.localfilename || "";
        this.destinationfilename = n.destinationfilename || "";
        this.gzip = n.gzip || false;
        this.bucketname = n.bucketname || "";

        var node = this;
        var GCS = this.gcsConfig ? this.gcsConfig.GCS : null;

        if (!GCS) {
            node.warn(RED._("gcs.warn.missing-credentials"));
            return;
        }
        if (GCS) {
            node.on("input", function(msg){
              var destinationfilename = node.destinationfilename || msg.destinationfilename;
              var localfilename = node.localfilename || msg.localfilename;

              node.CheckForBucket(node.bucketname)
              .then(function(data){
                var bucketExists = data[0];
                if (bucketExists) {
                  return node.UploadDataToBucket(node.bucketname, localfilename, destinationfilename, node.gzip)
                } else {
                  node.warn("Warning: Bucket \""+node.bucketname+"\" does not exist. Use the \"Create Bucket\" GCS node to create the bucket first.")
                }
              },
              function(err){
                node.err(err);
              })
              .then(function(data){
                node.send({"payload": true}); //TODO: Should this send a payload of false for error cases?
              }, function(err){
                node.warn(err);
              })
            })
        }
    }
    RED.nodes.registerType("gcs upload file",GCSFileUploadNode);

    GCSFileUploadNode.prototype.CheckForBucket = function(bucketName) {
      var node = this;

      var GCS = this.gcsConfig ? this.gcsConfig.GCS : null;

      if (!GCS) {
          node.warn(RED._("gcs.warn.missing-credentials"));
          return;
      }

      return GCS.bucket(bucketName).exists();
    }

    GCSFileUploadNode.prototype.UploadDataToBucket = function(bucketName, localPath, destinationPath, gzip) {
      var node = this;

      var GCS = this.gcsConfig ? this.gcsConfig.GCS : null;

      if (!GCS) {
          node.warn(RED._("gcs.warn.missing-credentials"));
          return;
      }

      // Reference an existing bucket.
      var bucket = GCS.bucket(bucketName);
      var uploadOptions = {
        "destination": destinationPath,
        "gzip": gzip
      }

      node.log("Uploading contents from \""+localPath+"\" to \"//"+bucketName+"/"+destinationPath+"\"");

      return bucket.upload(localPath, uploadOptions)
    }

    function GCSCreateBucket(n) {
      RED.nodes.createNode(this,n);
      this.gcsConfig = RED.nodes.getNode(n.gcs);
      this.bucketname = n.bucketname || "";

      var node = this;
      var GCS = this.gcsConfig ? this.gcsConfig.GCS : null;

      if (!GCS) {
          node.warn(RED._("gcs.warn.missing-credentials"));
          return;
      }
      if (GCS) {
        GCS.createBucket(bucketName, function(err, bucket) {
          if (!err) {
            node.log(bucketName+" was successfully created.")
            node.send({"payload": true})
          } else {
            node.warn(err);
            // node.send({"payload": false}) //TODO: Should this send a message on failure?
          }
        });
      }
    }
    RED.nodes.registerType("gcs create bucket", GCSCreateBucket);
};
