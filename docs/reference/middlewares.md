# Middlewares

Small helpers to parse the binary frames in the workers.

## Contrast

::: dranspose.middlewares.contrast

Example return dictionaries are
### stated
``` py
{'description': 'mesh sx -2 2 3 sy -2 2 4 0.1',
 'path': '/data/.../diff_1130_stream_test/raw/dummy',
 'scannr': 2,
 'snapshot': {'attenuator1_x': 0.048,
              'attenuator2_x': 0.067,
              'attenuator3_x': 0.536,
              ...
              'vfm_yaw': 0.15148492851039919,
              'xrf_x': 94.99875},
 'status': 'started'}
```

### running

``` py
OrderedDict([('sx', -2.000431059888797),
             ('sy', -2.0011940002441406),
             ('pseudo',
              {'analog_x': array([-1.99962707]),
               'analog_y': array([-1.99349905]),
               'analog_z': array([-0.00306218]),
               'x': array([-2.00405186]),
               'y': array([-2.00290304]),
               'z': array([0.00029938])}),
             ('panda0',
              {'COUNTER1.OUT_Value': array([0.]),
               'COUNTER4.OUT_Value': array([0.]),
               'COUNTER5.OUT_Value': array([0.]),
               'FMC_IN.VAL1_Mean': array([0.00061244]),
               'FMC_IN.VAL2_Mean': array([0.39992541]),
               'PCAP.TS_START_Value': array([0.00570229]),
               'PCAP.TS_TRIG_Value': array([0.10570229])}),
             ('xspress3',
              {'filename': '/data/.../diff_1130_stream_test/raw/dummy/scan_000002_xspress3.hdf5',
               'path': '/entry/instrument/xspress3/',
               'type': 'Link',
               'universal': True}),
             ('dt', 2.410903215408325),
             ('status', 'running')])
```

### finished

``` py
{'description': 'mesh sx -2 2 3 sy -2 2 4 0.1',
 'path': '/data/.../diff_1130_stream_test/raw/dummy',
 'scannr': 2,
 'snapshot': {'attenuator1_x': 0.048,
              'attenuator2_x': 0.066,
              ...
              'vfm_yaw': 0.15148492851039919,
              'xrf_x': 94.99875},
 'status': 'finished'}
```

## Xspress3

:::dranspose.middlewares.xspress