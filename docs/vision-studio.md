# Application studio and custom region detection

The top navigation separates **Clusters** (members, nodes, contribution controls, invites, recovery diagnostics) from **Applications** (select a compute cluster, create a private application, edit workflows, train and deploy a detector). Switching between these root-dashboard tabs keeps active browser contributors running. Navigating to a different page or application can stop a worker in the old page. Application URLs still open their private management screen and require explicit application access.

## Edit a deployed workflow

In **Applications → application → Workflows & runs**, choose **View deployed manifest** or **Edit deployed workflow**. Edit the JSON, preview, and save the replacement. The editor captures the original revision. A concurrent edit causes a conflict instead of overwriting someone else's work. Cancel the draft and reopen the deployed workflow to edit the newer revision. Running jobs retain the manifest revision they were queued from. Upload and public GitHub import remain available. This is a declarative workflow editor, not arbitrary code or web-page hosting.

Use **Settings & access** to rename an application without changing its URL, manage explicit app members, or delete the application. Cluster membership alone does not grant access to its private application dashboard.

## Train and deploy a detector

1. Open **Vision studio**, start an ML worker, or use **Invite vision contributors**. Friends can open that link and start a worker without installing software or creating an account. Revoke contributor links in the cluster's Active invites.
2. Load the included colored-block sample to exercise the flow, or add your own images. Draw boxes around every target object and choose a label. Label several regions containing none of the target objects as **background**. Numeric box controls support keyboard-only annotation. There can be one to four object classes, with at least two training boxes per class and two background boxes.
3. Mark at least one different annotated image as **Held-out validation**. It is excluded from fitting. Use varied training images and a representative validation set. Identical image bytes cannot be in both splits.
4. **Train detector on cluster** sends bounded RGB crops to the app's ML workers. Every crop is an independent leased task, with renewal, retry, expiry and fencing. The browser coordinator tab fits a small prototype head from returned features. Validation uses sliding-window predictions on held-out whole images, not the ground-truth crops.
5. Inspect the predicted boxes and precision/recall at IoU 0.5. These are single-threshold detection metrics, not mAP. A minimum similarity adjustment recomputes validation predictions. Once tuned, this validation set is no longer an independent test set.
6. The application owner can **Deploy detector**. This saves the name, thresholds, class counts and compact learned prototypes in the encrypted coordinator snapshot. The backbone revision is pinned. Existing models remain active until a replacement saves successfully.
7. Upload a genuinely new image under **Deployed detector**, then **Detect objects on cluster**. Download the normalized bounding boxes, labels, cosine similarity scores and model revision. Export/import the trained model to back it up or deploy it to another application. Remove a deployed model through the explicit confirmation control.

## What this model does

The first detector is `mesh-region-prototypes-v1`, a bounded transfer-learning baseline using the existing pinned MobileNetV4 classifier. It uses the classifier's **1,000 logits as descriptors**, mean-centers and L2-normalizes them, and averages normalized examples per label into normalized prototypes. This is not end-to-end backbone fine-tuning, YOLO/DETR training, or learned bounding-box regression.

For detection, the controlling tab proposes 35 windows in normalized image coordinates at full, three-quarter and half-image scales. Workers extract descriptors independently. The tab compares descriptors to saved prototypes, rejects background and weak matches, and applies per-class non-maximum suppression at IoU 0.3. Output is capped at 20 boxes. The default minimum cosine similarity is 0.65, with a 0.05 margin over background. Similarity is not calibrated confidence. Small, elongated, crowded or unfamiliar objects are likely to need a more capable detector and denser proposals. The synthetic sample tests the product flow, not real-world accuracy.

The expensive backbone inference is distributed across browser workers and uses WebGPU when supported, otherwise WASM. Prototype fitting, proposal generation, suppression and metrics run in the controlling browser. The Space schedules region tasks and holds temporary image/feature data. This workflow does not provide cluster-distributed gradient training.

## Data, permissions and limits

- Dataset images and annotations remain in the management tab. **Export labeled dataset** before leaving, refreshing or changing applications. No dataset is written to GitHub. Exported files include image pixels and should be treated as private data. Imports are limited to 8 MiB, 32 JPEG image records, and 16 boxes per image. Original uploads are resized to a maximum dimension of 768 pixels and re-encoded without original file metadata.
- Region crops are stretched to 96×96 RGB, consistently in training and prediction. Feature tasks opt into the upgraded worker protocol. Old workers skip these tasks instead of returning an incomplete result. Feature vectors must have exactly 1,000 finite, bounded values, and retries must match the original result.
- Region inputs and feature results are ephemeral, subject to the existing 30-minute image-run retention. A coordinator restart loses an in-progress vision run. Cancel attempts to delete the run immediately, while retention cleans up abandoned requests. Completed feature runs are deleted after the tab retrieves the results. Keep the management tab open during training and detection.
- Up to 1,024 regions per run, four runs per application, twelve globally, with a shared 72 MiB reservation budget. Feature storage is reserved up front. The model store allows one active detector per application and 64 globally. These limits bound this preview; they do not establish thousand-node throughput.
- Models use a fixed, validated format with up to five normalized 1,000-float prototypes encoded as little-endian f32 Base64. Model requests fit the existing 64 KiB route body limit. GET is restricted to app members, POST and DELETE to app owners. Writes and deletes require the current revision. Application deletion prunes the model. Operator accounts can train and run detection but cannot publish models.
- Contributors see assigned crops. Feature values come from one worker per crop and are shape-checked, not independently recomputed or attested. Use trusted contributors for sensitive data and reliable training. Hash pinning of the backbone protects model downloads, not the honesty of the machine running it.

## Validation

Unit tests cover model serialization, invalid data, background rejection, duplicate suppression, bounded proposals and one-to-one evaluation. Rust route tests cover model privacy, revision conflicts, deletion cleanup, bounded feature commits, stale-lease rejection and no per-crop snapshot writes. Browser tests exercise studio navigation, actual workflow editing, concurrent-edit rejection, real-model feature extraction, held-out box evaluation, model deployment, detection and reload.

The next model-quality comparison should use a representative labeled dataset with a separate untouched test split. Compare this baseline against an exported browser-compatible detector with a trainable head, tracking mAP, small-object recall, examples required, total crop traffic, cold/warm latency, worker churn and total training cost. Browser runtimes and export restrictions need evaluation before promising general detector training.

### Initial local result

On November 11, 2026, a real browser worker processed the synthetic eight-image sample (six training, two validation). With the default similarity threshold, held-out evaluation found both objects but emitted twelve boxes: precision 17%, recall 100% at IoU 0.5. This exposes substantial false positives even on a simple dataset. The model is an experimental baseline, and these results do not establish useful accuracy on real objects. A complete train, save, predict and reload flow is covered separately from model-quality claims.
