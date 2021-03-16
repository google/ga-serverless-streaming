/**
 * Copyright 2021 Google LLC
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

ga(function(tracker) {
  // Get sendHitTask
  var originalSendHitTask = tracker.get('sendHitTask');
  // Override sendHitTask
  tracker.set('sendHitTask', function(model) {
    // Execute the original task which sends the hit to GA
    originalSendHitTask(model);
    // Create xhr
    var XMLHttpRequest = window.XMLHttpRequest;
    var xhr = new XMLHttpRequest();
    // Test support
    if (!XMLHttpRequest || !("withCredentials" in xhr)) { 
      return;
    }
    // Open request
    xhr.open('POST', 'CLOUD_FUNCTION_TRIGGER_URL', true);
    // Set content type
    xhr.setRequestHeader('Content-Type', 'text/plain');
    // Retrieve the hit payload and manually add the referrer
    var hitPayload = model.get('hitPayload').concat('&referrer=' + encodeURIComponent(document.referrer));
    // Send the request
    xhr.send(hitPayload);
  });
});