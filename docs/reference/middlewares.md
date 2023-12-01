# Middlewares

Small helpers to parse the binary frames in the workers.

## Contrast

::: dranspose.middlewares.contrast

### Examples

``` py
ContrastStarted(
    status='started',
    path='/data/.../diff_1130_stream_test/raw/dummy',
    scannr=2,
    description='mesh sx -2 2 3 sy -2 2 4 0.1',
    snapshot={'attenuator1_x': 0.048, 
              'attenuator2_x': 0.067, 
              'attenuator3_x': 0.536, 
              'vfm_yaw': 0.15148492851039919, 
              'xrf_x': 94.99875}
)
```

``` py
ContrastRunning(
    status='running',
    dt=2.410903215408325,
    sx=-2.000431059888797,
    sy=-2.0011940002441406,
    pseudo={
        'x': array([-2.00405186]),
        'y': array([-2.00290304]),
        'z': array([0.00029938]),
        'analog_x': array([-1.99962707]),
        'analog_y': array([-1.99349905]),
        'analog_z': array([-0.00306218])
    },
    panda0={
        'COUNTER1.OUT_Value': array([0.]),
        'COUNTER2.OUT_Value': array([0.]),
        'COUNTER3.OUT_Value': array([0.]),
        'FMC_IN.VAL6_Mean': array([0.04644599]),
        'FMC_IN.VAL7_Mean': array([-0.02943451]),
        'FMC_IN.VAL8_Mean': array([0.01255371])
    },
    xspress3={
        'type': 'Link',
        'filename': '/data/.../diff_1130_stream_test/raw/dummy/scan_000002_xspress3.hdf5',
        'path': '/entry/instrument/xspress3/',
        'universal': True
    }
)
```

``` py
ContrastFinished(
    status='finished',
    path='/data/.../diff_1130_stream_test/raw/dummy',
    scannr=2,
    description='mesh sx -2 2 3 sy -2 2 4 0.1',
    snapshot={'attenuator1_x': 0.048, 
              'attenuator2_x': 0.066, 
              'vfm_yaw': 0.15148492851039919, 
              'xrf_x': 94.99875}
)

```

## Xspress3

:::dranspose.middlewares.xspress

### Examples

``` py
XspressStart(
    htype='header',
    filename='/data/.../diff_1130_stream_test/raw/dummy/scan_000002_xspress3.hdf5',
    overwritable=False
)
```

!!! note
    While the original stream sends 3 separate zmq frames (no multipart), this returns a single packet.

``` py
XspressImage(
    htype='image',
    frame=0,
    shape=[4, 4096],
    exptime=0.099999875,
    type='uint32',
    compression='none',
    data=array([[0, 0, 0, ..., 0, 0, 0],
       [0, 0, 0, ..., 0, 0, 0],
       [0, 0, 0, ..., 0, 0, 0],
       [0, 0, 0, ..., 0, 0, 0]], dtype=uint32),
    meta={
        'ocr': array([0.00000000e+00, 0.00000000e+00, 0.00000000e+00, 1.56250195e-15]),
        'AllEvents': array([2, 0, 0, 3], dtype=uint32),
        'AllGood': array([0, 0, 0, 1], dtype=uint32),
        'ClockTicks': array([7999990, 7999990, 7999990, 7999990], dtype=uint32),
        'TotalTicks': array([7999990, 7999990, 7999990, 7999990], dtype=uint32),
        'ResetTicks': array([ 0,  0,  0, 91], dtype=uint32),
        'event_widths': array([6, 6, 6, 6], dtype=int32),
        'dtc': array([1.00000175, 1.        , 1.        , 1.000014  ])
    }
)
```

``` py
XspressEnd(htype='series_end')
```