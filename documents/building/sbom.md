---
title: Software Bill of Materials
---

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Software Bill of Materials

The Linux Conan CI build generates a CycloneDX 1.6 Software Bill of Materials
using Conan's built-in `cyclone_1.6` deployer:

```bash
conan install . -o celix/*:build_all=True --deployer=cyclone_1.6 --deployer-folder=sbom -b missing -o *:shared=True
```

The generated `sbom/sbom-cyclonedx-1.6.json` file is published by CI as the
`celix-conan-sbom` artifact.

## Vulnerability scanning

CI downloads the generated `celix-conan-sbom` artifact in a separate job and
scans it with OSV-Scanner for known vulnerabilities. The scan is report-only:
known vulnerability findings do not fail the build. Scanner execution errors
still fail the scan job.

The machine-readable JSON result is published as the
`celix-osv-scanner-report` CI artifact.

The scan only covers components represented in the generated Conan SBOM that
OSV-Scanner can identify and match against known vulnerability data. A clean
report therefore does not prove that every Celix dependency is vulnerability-
free or that a reported vulnerability is exploitable in Celix.
