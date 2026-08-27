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

## Release SBOM

For an Apache Celix release, generate the SBOM from the same clean
`release-X.Y.Z` checkout that is used to prepare the source release. This keeps
the SBOM tied to the exact release candidate dependency graph.

Generate and give the SBOM a release-specific name:

```bash
conan install . -o celix/*:build_all=True --deployer=cyclone_1.6 --deployer-folder=sbom -b missing -o *:shared=True
cp sbom/sbom-cyclonedx-1.6.json celix-X.Y.Z-sbom.cdx.json
```

Conan's CycloneDX generator excludes build and test requirements by default, so
the release SBOM describes the non-build dependency graph unless the generator
is explicitly configured otherwise.

Sign the SBOM and create a SHA-512 checksum using the same release-manager key
and conventions as the source archive:

```bash
gpg --print-md SHA512 celix-X.Y.Z-sbom.cdx.json > celix-X.Y.Z-sbom.cdx.json.sha512
gpg --armor --output celix-X.Y.Z-sbom.cdx.json.asc --detach-sig celix-X.Y.Z-sbom.cdx.json
```

Before starting the release vote, copy the SBOM, its detached signature, and
its checksum to the Apache Celix release development area alongside
`celix-X.Y.Z.tar.gz` and its signature/checksum. The SBOM is therefore part of
the artifact set being voted on.

After a successful vote, promote the complete release directory from the
Apache `dev` distribution area to the `release` area without regenerating or
modifying the SBOM. The same signed SBOM may also be attached to the GitHub
release after the Apache release has been promoted.
