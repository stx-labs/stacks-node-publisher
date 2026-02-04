## [1.2.0](https://github.com/hirosystems/stacks-node-publisher/compare/v1.1.2...v1.2.0) (2025-12-23)

### Features

* add stream filter to receive only confirmed chain events ([#34](https://github.com/hirosystems/stacks-node-publisher/issues/34)) ([ff65fa8](https://github.com/hirosystems/stacks-node-publisher/commit/ff65fa8e6ce25ccc38b83d4a9eb3fd3ba5a0337b))

## [1.1.2](https://github.com/hirosystems/stacks-node-publisher/compare/v1.1.1...v1.1.2) (2025-07-02)

### Bug Fixes

* filter events sent to clients based on selected event stream type ([#27](https://github.com/hirosystems/stacks-node-publisher/issues/27)) ([995e639](https://github.com/hirosystems/stacks-node-publisher/commit/995e63931e4cd260fc680bc7a2d948cfd6711000))

## [1.1.2-beta.1](https://github.com/hirosystems/stacks-node-publisher/compare/v1.1.1...v1.1.2-beta.1) (2025-07-02)

### Bug Fixes

* filter events sent to clients based on selected event stream type ([#27](https://github.com/hirosystems/stacks-node-publisher/issues/27)) ([995e639](https://github.com/hirosystems/stacks-node-publisher/commit/995e63931e4cd260fc680bc7a2d948cfd6711000))

## [1.1.1](https://github.com/hirosystems/stacks-node-publisher/compare/v1.1.0...v1.1.1) (2025-04-01)

### Bug Fixes

* sanitize redis client name string to contain only valid characters ([#21](https://github.com/hirosystems/stacks-node-publisher/issues/21)) ([27e1275](https://github.com/hirosystems/stacks-node-publisher/commit/27e127560b2a0a6dd5deb3c98c65477e40b393be))

## [1.1.0](https://github.com/hirosystems/stacks-node-publisher/compare/v1.0.3...v1.1.0) (2025-03-31)

### Features

* per-client redis msg streams ([#13](https://github.com/hirosystems/stacks-node-publisher/issues/13)) ([c953b68](https://github.com/hirosystems/stacks-node-publisher/commit/c953b6823e3ce98ed101cae43aae0aac50f8e479))

## [1.0.3](https://github.com/hirosystems/stacks-node-publisher/compare/v1.0.2...v1.0.3) (2025-02-21)

### Bug Fixes

* use original timestamps from tsvs ([#11](https://github.com/hirosystems/stacks-node-publisher/issues/11)) ([4575d6b](https://github.com/hirosystems/stacks-node-publisher/commit/4575d6b81b9d4bfee919cb0a74aa618bec597768))

## [1.0.2](https://github.com/hirosystems/stacks-node-publisher/compare/v1.0.1...v1.0.2) (2025-01-09)

### Bug Fixes

* serialize writes ([#10](https://github.com/hirosystems/stacks-node-publisher/issues/10)) ([35a06dd](https://github.com/hirosystems/stacks-node-publisher/commit/35a06ddbd28e2337d490a7907b8554e3dd79f2ea))

## [1.0.1](https://github.com/hirosystems/stacks-node-publisher/compare/v1.0.0...v1.0.1) (2025-01-09)

### Bug Fixes

* redis client must have an error event handler ([#8](https://github.com/hirosystems/stacks-node-publisher/issues/8)) ([90f4957](https://github.com/hirosystems/stacks-node-publisher/commit/90f4957a7ddf6d2b9b6850157a361d37b6136a3d))

## 1.0.0 (2025-01-07)

### Features

* add /status endpoint to event-observer http server ([#5](https://github.com/hirosystems/stacks-node-publisher/issues/5)) ([af27ebc](https://github.com/hirosystems/stacks-node-publisher/commit/af27ebcf44e9b718da6110621a484bf49889b805))
* add first version of client lib ([#2](https://github.com/hirosystems/stacks-node-publisher/issues/2)) ([22d1300](https://github.com/hirosystems/stacks-node-publisher/commit/22d1300d31ded5552bcd70b00e165d42ea405d40))
* initial commit ([d85fafe](https://github.com/hirosystems/stacks-node-publisher/commit/d85fafe2ee3d55360ab77143318b15b7bb53d1c1))

### Bug Fixes

* cleanup code around redis client init and message queuing ([#1](https://github.com/hirosystems/stacks-node-publisher/issues/1)) ([f004277](https://github.com/hirosystems/stacks-node-publisher/commit/f004277990622a7b4a0111d2c353bc47d9c5e387))
* use sequence numbers for redis message IDs ([af9904d](https://github.com/hirosystems/stacks-node-publisher/commit/af9904d579636acb4224a8c229927741e9d49a74))
