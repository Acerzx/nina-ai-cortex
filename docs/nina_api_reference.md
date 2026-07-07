# Advanced API

**Version:** 2.2.15

**Base URL:** `http://localhost:1888/v2/api`

This is the API documentation for the NINA plugin Advanced API. Please use streaming instead of the default base64 encoding for images, as base64 support will be removed in the near future!


## Application

### `GET /version`

**Version**

Returns the installed plugin version.

### `GET /version/nina`

**NINA Version**

Returns the version of NINA.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `friendly` | query | boolean |  | If true, the version will be returned in a friendly format. |

### `GET /application-start`

**NINA start time**

Returns the time NINA was started.

### `GET /application/switch-tab`

**Switch Tab**

Switches the application tab

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `tab` | query | string | ✓ | The tab to switch to |

### `GET /application/get-tab`

**Get Tab**

Gets the current application tab

### `GET /application/plugins`

**Plugins**

Get a list of installed plugins. This is useful for example if you want to use an integrated plugin like livestack or TPPA

### `GET /application/logs`

**Logs**

Get a list of the last N log entries, this will ignore the header of the file. The endpoint is limited by the log level set in NINA

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `lineCount` | query | integer | ✓ | Return the last N lines of the log file, with N being the lineCount |
| `level` | query | string |  | Filter the log entries by level. This uses the provided level as a minimum level, so level INFO will return INFO, WARNING and ERROR entries |

### `GET /application/screenshot`

**Screenshot**

Takes a screenshot

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `resize` | query | boolean |  | Whether to resize the screenshot. |
| `quality` | query | integer |  | The quality of the screenshot, ranging from 1 (worst) to 100 (best). -1 or omitted for png |
| `size` | query | string |  | The size of the screenshot ([width]x[height]). Requires resize to be true. |
| `scale` | query | number |  | The scale of the screenshot. Requires resize to be true. |
| `stream` | query | boolean |  | Stream the image to the client. This will stream the image in image/jpg or image/png format. |


## Camera

### `GET /equipment/camera/info`

**Information**

This endpoint returns relevant information about the camera.

### `GET /equipment/camera/connect`

**Connect**

Connect to Camera

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `to` | query | string |  | The Id of the device that should be connected. |

### `GET /equipment/camera/disconnect`

**Disconnect**

This endpoint disconnects the camera.

### `GET /equipment/camera/list-devices`

**List Devices**

List all devices which can be connected

### `GET /equipment/camera/rescan`

**Rescan Devices**

Rescans for new devices, and returns a list of all available devices

### `GET /equipment/camera/set-readout`

**Set readout mode**

This endpoint sets the readout mode of the camera.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `mode` | query | integer | ✓ | The readout mode to set. |

### `GET /equipment/camera/set-readout/image`

**Set readout mode for normal images**

This endpoint sets the readout mode for normal images.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `mode` | query | integer | ✓ | The readout mode to set. |

### `GET /equipment/camera/set-readout/snapshot`

**Set readout mode for snapshots**

This endpoint sets the readout mode for snapshots.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `mode` | query | integer | ✓ | The readout mode to set. |

### `GET /equipment/camera/cool`

**Cooling**

This endpoint cools the camera.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `temperature` | query | number | ✓ | The temperature to cool the camera to. |
| `minutes` | query | number | ✓ | The minimum duration to cool the camera. -1 for default duration |
| `cancel` | query | boolean |  | Whether to cancel the cooling process. |

### `GET /equipment/camera/warm`

**Warming**

This endpoint warms the camera.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `minutes` | query | number | ✓ | The minimum duration to warm the camera. -1 for default duration |
| `cancel` | query | boolean |  | Whether to cancel the warming process. |

### `GET /equipment/camera/abort-exposure`

**Exposure Abort**

This endpoint aborts the current exposure.

### `GET /equipment/camera/dew-heater`

**Dew Heater Control**

This endpoint sets the dew heater.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `power` | query | boolean | ✓ | Whether to turn the dew heater on or off. |

### `GET /equipment/camera/usb-limit`

**Set USB limit**

This endpoint sets the usb limit of the camera

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `limit` | query | string | ✓ | The USB limit. Has to be between USBLimitMin and USBLimitMax. |

### `GET /equipment/camera/set-binning`

**Set Binning**

This endpoint sets the binning of the camera, if the specified binning is supported

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `binning` | query | string | ✓ | The binning mode |

### `GET /equipment/camera/capture`

**Capture / Platesolve**

This endpoint captures and/or returns an image. Can optionally solve the image.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `solve` | query | boolean |  | Whether to solve the image. |
| `duration` | query | number |  | The duration of the exposure. If omitted the exposure time for plate solving will be used. |
| `gain` | query | number |  | The gain to use for the exposure. If omitted, the camera's default gain will be used. |
| `getResult` | query | boolean |  | Whether to get the result. |
| `resize` | query | boolean |  | Whether to resize the image. |
| `quality` | query | number |  | The quality of the image, ranging from 1 (worst) to 100 (best). -1 or omitted for png |
| `size` | query | string |  | The size of the image ([width]x[height]). Requires resize to be true. |
| `scale` | query | number |  | The scale of the image. Requires resize to be true. |
| `stream` | query | boolean |  | Stream the image to the client. This will only stream the image in image/jpg or image/png format. The platesolve result is not included. |
| `omitImage` | query | boolean |  | Omit the image from the response. This will only send the platesolve result, if the image was platesolved. Use it if you do not care about the image, only about the platesolve result. |
| `waitForResult` | query | boolean |  | Wait for the capture to finish and then return the result. This will immediately take into account all the settings you would otherwise use together with getResult, like resize and stream. |
| `targetName` | query | string |  | The name of the target that is being captured. Useful for livestacking as an example. |
| `save` | query | boolean |  | Save the image to the disk. This needs to be set, when capturing the image. |
| `onlyAwaitCaptureCompletion` | query | boolean |  | Use this if you want to capture images at a high rate, the endpoint will return immediately once the camera is ready to take a picture again. The image preparation, platesolve and save will continue to run in the background, so the image wont be available until these have completed as well. |
| `onlySaveRaw` | query | boolean |  | Use if you do not need the other capture endpoints, but want to save the raw image. Useful for high frequency captures. |
| `skipAutoStretch` | query | boolean |  | Use if you do not want the autostretch in the preview window. Useful for high frequency captures. |
| `imageType` | query | string |  | The image type (light, dark, bias, flat, snapshot) |

### `GET /equipment/camera/capture/statistics`

**Capture Statistics**

This endpoint returns the image statistics for the last captured image.


## Dome

### `GET /equipment/dome/info`

**Information**

Get Dome information

### `GET /equipment/dome/connect`

**Connect**

Connect to Dome

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `to` | query | string |  | The Id of the device that should be connected. |

### `GET /equipment/dome/disconnect`

**Disconnect**

Disconnect the Dome

### `GET /equipment/dome/list-devices`

**List Devices**

List all devices which can be connected

### `GET /equipment/dome/rescan`

**Rescan Devices**

Rescans for new devices, and returns a list of all available devices

### `GET /equipment/dome/open`

**Open Shutter**

Open Dome Shutter

### `GET /equipment/dome/close`

**Close Shutter**

Close Dome Shutter

### `GET /equipment/dome/stop`

**Stop Dome Movement**

Stop Dome movement.

### `GET /equipment/dome/set-follow`

**Set Dome Follow**

Start or stop the dome to follow the telescope

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `enabled` | query | boolean |  | Enable or disable dome follow |

### `GET /equipment/dome/sync`

**Sync Dome To Telescope**

Sync dome to telescope coordinates

### `GET /equipment/dome/slew`

**Slew**

Slew dome to specified azimuth

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `azimuth` | query | number |  | Azimuth in degrees |
| `waitToFinish` | query | boolean |  | Wait until slew is finished |

### `GET /equipment/dome/set-park-position`

**Set Park Position**

Sets the current dome position as park position, if supported

### `GET /equipment/dome/park`

**Park**

Parks the dome

### `GET /equipment/dome/home`

**Home**

Homes the dome


## Equipment

### `GET /equipment/info`

**Equipment info**

This endpoint returns all equipment info bundled updated.


## Event Websocket

### `GET /event-history`

**Event History**

Get event history


## FilterWheel

### `GET /equipment/filterwheel/info`

**Information**

Get Filterwheel information

### `GET /equipment/filterwheel/connect`

**Connect**

Connect to Filterwheel

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `to` | query | string |  | The Id of the device that should be connected. |

### `GET /equipment/filterwheel/disconnect`

**Disconnect**

Disconnect the filterwheel

### `GET /equipment/filterwheel/list-devices`

**List Devices**

List all devices which can be connected

### `GET /equipment/filterwheel/rescan`

**Rescan Devices**

Rescans for new devices, and returns a list of all available devices

### `GET /equipment/filterwheel/change-filter`

**Change Filter**

Change Filter

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `filterId` | query | integer | ✓ | The filter to change to. |

### `GET /equipment/filterwheel/add-filter`

**Add Filter**

Add Filter

### `GET /equipment/filterwheel/remove-filter`

**Remove Filter**

Remove a filter from the list of available filters

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `filterId` | query | integer | ✓ | The filter to remove. |

### `GET /equipment/filterwheel/filter-info`

**Filter Information**

Get Filterwheel Filter

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `filterId` | query | integer | ✓ | The filter to get. |


## Flat Panel

### `GET /equipment/flatdevice/info`

**Information**

Get information about the flat panel, Coverstate represents the following values&#58; 0&#58; Unknown, 1&#58; NeitherOpenNorClosed, 2&#58; Closed, 3&#58; Open, 4&#58; Error, 5&#58; Not present

### `GET /equipment/flatdevice/connect`

**Connect**

Connect the flat panel

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `to` | query | string |  | The Id of the device that should be connected. |

### `GET /equipment/flatdevice/disconnect`

**Disconnect**

Disconnect the flat panel

### `GET /equipment/flatdevice/list-devices`

**List Devices**

List all devices which can be connected

### `GET /equipment/flatdevice/rescan`

**Rescan Devices**

Rescans for new devices, and returns a list of all available devices

### `GET /equipment/flatdevice/set-light`

**Set Light**

Set the light on the flatdevice

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `on` | query | boolean | ✓ | The actual parameter name is "on", but for some reason the documentation automatically renames it to true... |

### `GET /equipment/flatdevice/set-cover`

**Set Cover**

Set the cover to the specified position

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `closed` | query | boolean | ✓ | Indicates if the cover should be closed or open |

### `GET /equipment/flatdevice/set-brightness`

**Set Brightness**

Set Brightness

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `brightness` | query | integer | ✓ | Brightness |


## Flats

### `GET /flats/skyflat`

**Sky flats**

Start capturing sky flats. This requires the camera and mount to be connected. Any omitted parameter will default to the instruction default.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `count` | query | integer | ✓ | The number of flats to capture |
| `minExposure` | query | number |  | The minimum exposure time to use for the flats, in seconds |
| `maxExposure` | query | number |  | The maximum exposure time to use for the flats, in seconds |
| `histogramMean` | query | number |  | The mean to use for the histogram |
| `meanTolerance` | query | number |  | The tolerance to use for the histogram |
| `dither` | query | boolean |  | Whether to dither the flats |
| `filterId` | query | integer |  | The filter to use for the flats. The current filter will be used if this is not specified |
| `binning` | query | string |  | The binning to use for the flats |
| `gain` | query | integer |  | The gain to use for the flats. The camera gain will be used if this is not specified |
| `offset` | query | integer |  | The offset to use for the flats. The camera offset will be used if this is not specified |

### `GET /flats/auto-brightness`

**Auto Brightness Flats**

Start capturing auto brightness flats. This requires the camera to be connected. NINA will pick the best flat panel brightness for a fixed exposure time. Any omitted parameter will default to the instruction default.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `count` | query | integer | ✓ | The number of flats to capture |
| `exposureTime` | query | number | ✓ | The exposure time to use for the flats, in seconds |
| `minBrightness` | query | integer |  | The minimum flat panel brightness to use for the flats |
| `maxBrightness` | query | integer |  | The maximum flat panel brightness to use for the flats |
| `histogramMean` | query | number |  | The mean to use for the histogram |
| `meanTolerance` | query | number |  | The tolerance to use for the histogram |
| `filterId` | query | integer |  | The filter to use for the flats. The current filter will be used if this is not specified |
| `binning` | query | string |  | The binning to use for the flats |
| `gain` | query | integer |  | The gain to use for the flats. The camera gain will be used if this is not specified |
| `offset` | query | integer |  | The offset to use for the flats. The camera offset will be used if this is not specified |
| `keepClosed` | query | boolean |  | Whether to keep the flat panel closed after taking the flats |

### `GET /flats/auto-exposure`

**Auto Exposure Flats**

Start capturing auto exposure flats. This requires the camera to be connected. NINA will pick the best exposure time for a fixed flat panel brightness. Any omitted parameter will default to the instruction default.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `count` | query | integer | ✓ | The number of flats to capture |
| `brightness` | query | number | ✓ | The flat panel brightness |
| `minExposure` | query | number |  | The minimum exposure time to use for the flats, in seconds |
| `maxExposure` | query | number |  | The maximum exposure time to use for the flats, in seconds |
| `histogramMean` | query | number |  | The mean to use for the histogram |
| `meanTolerance` | query | number |  | The tolerance to use for the histogram |
| `filterId` | query | integer |  | The filter to use for the flats. The current filter will be used if this is not specified |
| `binning` | query | string |  | The binning to use for the flats |
| `gain` | query | integer |  | The gain to use for the flats. The camera gain will be used if this is not specified |
| `offset` | query | integer |  | The offset to use for the flats. The camera offset will be used if this is not specified |
| `keepClosed` | query | boolean |  | Whether to keep the flat panel closed after taking the flats |

### `GET /flats/trained-dark-flat`

**Trained Darkflats**

Start capturing darkflats based on previous training in NINA. This requires the camera to be connected. Any omitted parameter will default to the instruction default.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `count` | query | integer | ✓ | The number of darkflats to capture |
| `filterId` | query | integer |  | The filter to use for the darkflats. The current filter will be used if this is not specified |
| `binning` | query | string |  | The binning to use for the darkflats |
| `gain` | query | integer |  | The gain to use for the darkflats. The camera gain will be used if this is not specified |
| `offset` | query | integer |  | The offset to use for the darkflats. The camera offset will be used if this is not specified |
| `keepClosed` | query | boolean |  | Whether to keep the flat panel closed after taking the darkflats |

### `GET /flats/trained-flat`

**Trained Flats**

Start capturing flats based on previous training done in NINA. This requires the camera to be connected. Any omitted parameter will default to the instruction default.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `count` | query | integer | ✓ | The number of flats to capture |
| `filterId` | query | integer |  | The filter to use for the flats. The current filter will be used if this is not specified |
| `binning` | query | string |  | The binning to use for the flats |
| `gain` | query | integer |  | The gain to use for the flats. The camera gain will be used if this is not specified |
| `offset` | query | integer |  | The offset to use for the flats. The camera offset will be used if this is not specified |
| `keepClosed` | query | boolean |  | Whether to keep the flat panel closed after taking the flats |

### `GET /flats/status`

**Status**

Returns the current status of the flat taking process, Running or Finished.

### `GET /flats/stop`

**Stop**

Stop a running flat taking process


## Focuser

### `GET /equipment/focuser/info`

**Information**

Get the focuser info

### `GET /equipment/focuser/connect`

**Connect**

Connect to Focuser

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `to` | query | string |  | The Id of the device that should be connected. |

### `GET /equipment/focuser/disconnect`

**Disconnect**

Disconnect the focuser

### `GET /equipment/focuser/list-devices`

**List Devices**

List all devices which can be connected

### `GET /equipment/focuser/rescan`

**Rescan Devices**

Rescans for new devices, and returns a list of all available devices

### `GET /equipment/focuser/move`

**Move**

Move the focuser

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `position` | query | integer | ✓ | Position to move to |

### `GET /equipment/focuser/stop-move`

**Stop Movement**

Stops the movement of the focuser, this only works if the movement was started using one of the api endpoints

### `GET /equipment/focuser/auto-focus`

**Auto Focus**

Start an AF

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `cancel` | query | boolean |  | Can be used to cancel a running autofocus (If it was started using the api) |

### `GET /equipment/focuser/last-af`

**Get Last Autofocus**

Get last autofocus


## Framing Assistant

### `GET /astro-util/moon-separation`

**Moon Separation**

Calculate the moon separation for the current time and location for given coordinates. This may not agree with NINA but it seems that this implementation is more accurate.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `ra` | query | number | ✓ | Right Ascension in degrees |
| `dec` | query | number | ✓ | Declination in degrees |

### `GET /framing/info`

**Information**

Get framing assistant information

### `GET /framing/set-source`

**Set Source**

Set framing assistant source. This requires the framing assistant to be initalized, which can by achieved by openening it once.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `source` | query | string | ✓ | The source to set |

### `GET /framing/set-coordinates`

**Set Coordinates**

Set framing assistant coordinates. This requires the framing assistant to be initalized, which can by achieved by openening it once.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `RAangle` | query | number | ✓ | The RA angle to set |
| `DecAngle` | query | number | ✓ | The Dec angle to set |

### `GET /framing/slew`

**Slew**

Slew the mount to the current coordinates. This requires the framing assistant to be initalized, which can by achieved by openening it once.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `slew_option` | query | string |  | The slew option to use. Possible values&#58; Center, Rotate. If this is omitted, it is a simple slew. |
| `waitForResult` | query | boolean |  | Whether to wait for the slew to finish |

### `GET /framing/set-rotation`

**Set Rotation**

Set framing assistant rotation. This requires the framing assistant to be initalized, which can by achieved by openening it once.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `rotation` | query | number | ✓ | The rotation to set |

### `GET /framing/determine-rotation`

**Determine Rotation**

Determine rotation from camera. This does nothing else than what the button in the framing assistant does. If waitForResult is set to true, the method will wait until the rotation is determined. This will only work if an image is loaded in the framing assistant

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `waitForResult` | query | boolean |  | Whether to wait for the result. |


## Guider

### `GET /equipment/guider/info`

**Information**

Get guider information

### `GET /equipment/guider/connect`

**Connect**

Connect to Guider

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `to` | query | string |  | The Id of the device that should be connected. |

### `GET /equipment/guider/disconnect`

**Disconnect**

Disconnect the guider

### `GET /equipment/guider/list-devices`

**List Devices**

List all devices which can be connected

### `GET /equipment/guider/rescan`

**Rescan Devices**

Rescans for new devices, and returns a list of all available devices

### `GET /equipment/guider/start`

**Start Guiding**

Start guiding

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `calibrate` | query | boolean |  | Whether to force the guider to calibrate before start guiding |

### `GET /equipment/guider/stop`

**Stop Guiding**

Stop guiding

### `GET /equipment/guider/clear-calibration`

**Clear Calibration**

Clears the calibration data, forces the guider to recalibrate when it starts guiding

### `GET /equipment/guider/graph`

**Graph**

Gets the last n guide steps needed to construct a graph, with n being the number of saved steps as configured on the graph in NINA


## Image

### `GET /image/{index}`

**Get Image**

Get image

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `index` | path | integer | ✓ | The index of the image to get |
| `resize` | query | boolean |  | Whether to resize the image. |
| `quality` | query | integer |  | The quality of the image, ranging from 1 (worst) to 100 (best). -1 or omitted for png |
| `size` | query | string |  | The size of the image ([width]x[height]). Requires resize to be true. |
| `scale` | query | number |  | The scale of the image. Requires resize to be true. |
| `factor` | query | number |  | The stretch factor to apply, between 0 and 1. Defaults to what is configured in the profile. |
| `blackClipping` | query | number |  | The black clipping to apply. Defaults to what is configured in the profile. |
| `unlinked` | query | boolean |  | Indicates if the stretch should be unlinked. Defaults to what is configured in the profile. |
| `stream` | query | boolean |  | Stream the image to the client. This will stream the image in image/jpg or image/png format. |
| `debayer` | query | boolean |  | Indicates if the image should be debayered |
| `bayerPattern` | query | string |  | What bayer pattern to use for debayering, if debayer is true. If this is not specified, the api will try to use the bayer pattern that is configured in the profile as its first option, if that is set to Auto, the api will try to use the bayer pattern reported by the camera if it is connected. The fallback value is Monochrome. |
| `autoPrepare` | query | boolean |  | Setting this to true will leave all processing up to NINA and you will recieve exactly the same image as you see in NINA. All other parameters related to image processing will have no effect. |
| `imageType` | query | string |  | Filter the image history by image type. This is useful if you got the index of an image using the history with a filter and now want to get the image. E.g. the 3rd flat in the image history is the 3rd flat in the image endpoint. |
| `raw_fits` | query | boolean |  | Whether to send the image (without streaming) as a raw FITS format. Will return an error if the image is not of type FITS. |

### `GET /image/{index}/solve`

**Solve image**

Solves the specified image, the result is returned immediately (blocking request)

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `index` | path | integer | ✓ | The index of the image to solve |
| `imageType` | query | string |  | Filter the image history by image type. This is useful if you got the index of an image using the history with a filter and now want to get the image. E.g. the 3rd flat in the image history is the 3rd flat in the image endpoint. |

### `GET /prepared-image/solve`

**Solve prepared image**

Solves the prepared image, the result is returned immediately (blocking request)

### `GET /prepared-image`

**Get Prepared Image**

Get the last prepared image. This is the image that is shown in NINA in the image dockable.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `resize` | query | boolean |  | Whether to resize the image. |
| `quality` | query | integer |  | The quality of the image, ranging from 1 (worst) to 100 (best). -1 or omitted for png |
| `size` | query | string |  | The size of the image ([width]x[height]). Requires resize to be true. |
| `scale` | query | number |  | The scale of the image. Requires resize to be true. |
| `factor` | query | number |  | The stretch factor to apply, between 0 and 1. Defaults to what is configured in the profile. |
| `blackClipping` | query | number |  | The black clipping to apply. Defaults to what is configured in the profile. |
| `unlinked` | query | boolean |  | Indicates if the stretch should be unlinked. Defaults to what is configured in the profile. |
| `debayer` | query | boolean |  | Indicates if the image should be debayered |
| `bayerPattern` | query | string |  | What bayer pattern to use for debayering, if debayer is true. If this is not specified, the api will try to use the bayer pattern that is configured in the profile as its first option, if that is set to Auto, the api will try to use the bayer pattern reported by the camera if it is connected. The fallback value is Monochrome. |
| `autoPrepare` | query | boolean |  | Setting this to true will leave all processing up to NINA and you will recieve exactly the same image as you see in NINA. All other parameters related to image processing will have no effect. |

### `GET /image-history`

**Get Image History**

Get image history. Only one parameter is required

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `all` | query | boolean |  | Whether to get all images or only the current image |
| `index` | query | integer |  | The index of the image to get |
| `count` | query | boolean |  | Whether to count the number of images |
| `imageType` | query | string |  | Filter by image type. This will restrict the result to images of the specified type, meaning that count for example will only count images of the specified type. If this is omitted, all images are included. |

### `GET /image/thumbnail/{index}`

**Get Thumbnail**

Get the thumbnail of an image. This requies Create Thumbnails to be enabled in NINA. Otherwise, use the image endpoint and resize the image. This thumbnail has a width of 256px.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `index` | path | integer | ✓ | The index of the image to get |
| `imageType` | query | string |  | Filter the image history by image type. This is useful if you got the index of an image using the history with a filter and now want to get the image. E.g. the 3rd flat in the image history is the 3rd flat in the image endpoint. |


## Livestack

### `GET /livestack/status`

**Request Livestack status**

Requests the current status of the Livestack, requires Livestack >= v1.0.1.7. Note that this method cannot fail, even if the livestack plugin is not installed or something went wrong. Livestack status is tracked by the server and we can get always the last status reported by nina.plugin.livestack.

### `GET /livestack/start`

**Start Livestack**

Starts Livestack, requires Livestack >= v1.0.0.9. Note that this method cannot fail, even if the livestack plugin is not installed or something went wrong. This simply issues a command to start the livestack.

### `GET /livestack/stop`

**Stop Livestack**

Stops Livestack, requires Livestack >= v1.0.0.9. Note that this method cannot fail, even if the livestack plugin is not installed or something went wrong. This simply issues a command to stop the livestack.

### `GET /livestack/image/available`

**Available Stacks**

Livestack >= v1.0.0.9. Returns a list of available stacks, which can be retrieved

### `GET /livestack/image/{target}/{filter}`

**Get Stacked Image**

Gets the stacked image from the livestack plugin for a given target and filter.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `target` | path | string | ✓ |  |
| `filter` | path | string | ✓ |  |
| `resize` | query | boolean |  | Whether to resize the image. |
| `quality` | query | integer |  | The quality of the image, ranging from 1 (worst) to 100 (best). -1 or omitted for png |
| `size` | query | string |  | The size of the image ([width]x[height]). Requires resize to be true. |
| `scale` | query | number |  | The scale of the image. Requires resize to be true. |
| `stream` | query | boolean |  | Stream the image to the client. This will stream the image in image/jpg or image/png format. |

### `GET /livestack/image/{target}/{filter}/info`

**Get Stacked Image Info**

Gets information about the stacked image, like filter, target and stack count.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `target` | path | string | ✓ |  |
| `filter` | path | string | ✓ |  |


## Mount

### `GET /equipment/mount/info`

**Information**

Get mount information

### `GET /equipment/mount/connect`

**Connect**

Connect to Mount

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `to` | query | string |  | The Id of the device that should be connected. |

### `GET /equipment/mount/disconnect`

**Disconnect**

Disconnect the mount

### `GET /equipment/mount/list-devices`

**List Devices**

List all devices which can be connected

### `GET /equipment/mount/rescan`

**Rescan Devices**

Rescans for new devices, and returns a list of all available devices

### `GET /equipment/mount/home`

**Home**

Home the mount

### `GET /equipment/mount/tracking`

**Tracking Mode**

Set tracking mode

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `mode` | query | integer | ✓ | The tracking mode to set. 0&#58; Siderial, 1&#58; Lunar, 2&#58; Solar, 3&#58; King, 4&#58; Stopped |

### `GET /equipment/mount/park`

**Park**

Park the mount

### `GET /equipment/mount/unpark`

**Unpark**

Unpark the mount

### `GET /equipment/mount/flip`

**Flip**

Performs a meridian flip to the current coordinates. This will only flip the mount if it is needed, it will not force the mount to flip

### `GET /equipment/mount/slew`

**Slew**

Performs a slew to the provided coordinates

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `ra` | query | number | ✓ | The RA angle of the target in degree |
| `dec` | query | number | ✓ | The Dec angle of the target in degree |
| `waitForResult` | query | boolean |  | Whether to wait for the slew to finish |
| `center` | query | boolean |  | Whether to center the telescope on the target |
| `rotate` | query | boolean |  | Whether to perform a center and rotate |
| `rotationAngle` | query | number |  | The rotation angle in degree |

### `GET /equipment/mount/slew/stop`

**Stop Slew**

Stops any slew, even if it was not started using the api. However this is mainly useful for slews you issued yourself, since it doesn't completely abort slew&centers started by NINA. Therefore the recommended use is to stop simple slews without center or rotate. With center or rotate, this may take a few seconds to complete.

### `GET /equipment/mount/set-park-position`

**Set Park Position**

Sets the current mount position as park position. This requires the mount to be unparked.

### `GET /equipment/mount/sync`

**Sync**

Sync the scope, either by manually supplying the coordinates or by solving and syncing. If the coordinates are omitted, a platesolve will be performed.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `ra` | query | number |  | Right ascension in degrees |
| `dec` | query | number |  | Declination in degrees |


## Plugin

### `GET /plugin/settings`

**Plugin settings**

This endpoint returns the plugin settings.


## Profile

### `GET /profile/show`

**Show Profile**

Shows the profile

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `active` | query | boolean |  | Whether to show the active profile or a list of all available profiles |

### `GET /profile/change-value`

**Change Profile Value**

Changes a value in the profile

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `settingpath` | query | string | ✓ | The path to the setting to change. (eg. CameraSettings-PixelSize). This refers the the profile structure like it is recieved when using /profile/show?active=true. Seperate each object with a dash (-) |
| `newValue` | query | object | ✓ | The new value to set |

### `GET /profile/switch`

**Switch Profile**

Switches the profile

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `profileid` | query | string | ✓ | The id of the profile to switch to. This id can be retrieved using show with active=false |

### `GET /profile/horizon`

**Horizon**

Gets the horizon for the active profile


## Rotator

### `GET /equipment/rotator/info`

**Information**

Get rotator information

### `GET /equipment/rotator/connect`

**Connect**

Connect to Rotator

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `to` | query | string |  | The Id of the device that should be connected. |

### `GET /equipment/rotator/disconnect`

**Disconnect**

Disconnect the rotator

### `GET /equipment/rotator/list-devices`

**List Devices**

List all devices which can be connected

### `GET /equipment/rotator/rescan`

**Rescan Devices**

Rescans for new devices, and returns a list of all available devices

### `GET /equipment/rotator/move`

**Move**

Move the rotator

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `position` | query | number | ✓ | The position to move to |

### `GET /equipment/rotator/move-mechanical`

**Move Mechanically**

Move the rotator mechanically

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `position` | query | number | ✓ | The position to move to |

### `GET /equipment/rotator/reverse`

**Reverse**

Reverses the direction of the rotator

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `reverseDirection` | query | boolean | ✓ | If reverse should be on or off |

### `GET /equipment/rotator/set-mechanical-range`

**Set Range**

Sets the mechanical range of the rotator to full, 180° (half) or 90° (quarter)

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `range` | query | string | ✓ | The range to set |
| `rangeStartPosition` | query | number |  | The mechanical position of the start of the set range (only applies to half and quarter) |

### `GET /equipment/rotator/stop-move`

**Stop Movement**

Stops the movement of the rotator, this only works if the movement was started using one of the api endpoints


## Safety Monitor

### `GET /equipment/safetymonitor/info`

**Information**

Get safety monitor information

### `GET /equipment/safetymonitor/connect`

**Connect**

Connect to safety monitor

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `to` | query | string |  | The Id of the device that should be connected. |

### `GET /equipment/safetymonitor/disconnect`

**Disconnect**

Disconnect the safety monitor

### `GET /equipment/safetymonitor/list-devices`

**List Devices**

List all devices which can be connected

### `GET /equipment/safetymonitor/rescan`

**Rescan Devices**

Rescans for new devices, and returns a list of all available devices


## Sequence

### `GET /sequence/json`

**JSON**

Get sequence as json. This endpoint is generally advised to use over state since it gives more reliable results.

### `GET /sequence/state`

**Complete Sequence**

Get sequence as json. This is similar to the json endpoint, however the returned sequence is much more elaborate and also supports plugins. The returned json from /json is not directly compatible with this endpoint. Use this endpoint (not json!) as reference for sequence editing! In general however I recommend using the json endpoint as it gives more reliable results, so use this if you do not need the extra functionality.

### `GET /sequence/edit`

**Edit a Sequence**

This works similary to profile/set-value. Note that this mainly supports fields that expect simple types like strings, numbers etc, and may not work for things like enums or objects (filter, time source, ...). This is an experimental feature, and it could have unexpected side effects or simply not work for some instructions or fields. If you encounter any bugs (except that it is not working with enums or objects), feel free to create an issue on github or contact me on the NINA discord.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `path` | query | string | ✓ | The path to the property that should be updated. Use `GlobalTriggers`, `Start`, `Imaging`, `End` for the sequence root containers. Then use the name of the property or the index of the item in a list, seperated with `-`. The example would set the exposure time of a Take Exposure instruction, contained in a DSO container, to 20s. Use `sequence/state` as reference, not `sequence/json`! |
| `value` | query | string | ✓ | The new value |

### `GET /sequence/start`

**Start**

Start Advanced Sequence.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `skipValidation` | query | boolean |  | Skip validation of the sequence |

### `GET /sequence/skip`

**Skip**

Skip in the sequence. You can skip to the end container of the sequence, to the imaging container or the current running items.

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `type` | query | string | ✓ | Skip to the end, to the imaging or to the current running items |

### `GET /sequence/stop`

**Stop**

Stop Advanced Sequence.

### `GET /sequence/reset`

**Reset Sequence**

Reset Advanced Sequence.

### `GET /sequence/list-available`

**Available Sequences**

List available sequences.

### `GET /sequence/set-target`

**Set Target**

Set the target of any one of the active target containers in the sequence

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `name` | query | string | ✓ | The target name |
| `ra` | query | number | ✓ | The RA coordinate in degrees |
| `dec` | query | number | ✓ | The DEC coordinate in degrees |
| `rotation` | query | number | ✓ | The target rotation |
| `index` | query | integer | ✓ | The index of the target container to update |

### `GET /sequence/load`

**Load Sequence from file**

Loads a sequence from a file from the default sequence folders, the names can be retrieved using the `sequence/list-available` endpoint

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `sequenceName` | query | string | ✓ | The name of the sequence to load |

### `POST /sequence/load`

**Load Sequence from JSON**

Loads a sequence from a JSON supplied by the client in the request body


## Switch

### `GET /equipment/switch/info`

**Information**

Get switch information

### `GET /equipment/switch/connect`

**Connect**

Connect to Switch

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `to` | query | string |  | The Id of the device that should be connected. |

### `GET /equipment/switch/disconnect`

**Disconnect**

Disconnect the switch

### `GET /equipment/switch/list-devices`

**List Devices**

List all devices which can be connected

### `GET /equipment/switch/rescan`

**Rescan Devices**

Rescans for new devices, and returns a list of all available devices

### `GET /equipment/switch/set`

**Set Value**

Set switch value

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `index` | query | integer | ✓ | The index of the switch to set |
| `value` | query | number | ✓ | The value to set |


## Weather

### `GET /equipment/weather/info`

**Information**

Get weather information

### `GET /equipment/weather/connect`

**Connect**

Connect to weather source

#### Parameters

| Name | Location | Type | Required | Description |
|------|----------|------|----------|-------------|
| `to` | query | string |  | The Id of the device that should be connected. |

### `GET /equipment/weather/disconnect`

**Disconnect**

Disconnect the weather

### `GET /equipment/weather/list-devices`

**List Weather Sources**

List all weather sources which can be connected

### `GET /equipment/weather/rescan`

**Rescan Sources**

Rescans for new weather sources, and returns a list of all available sources

