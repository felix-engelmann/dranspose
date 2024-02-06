# Sardana integration

The scanning software sardana allows to create pre-scan hooks to notify dranspose of an upcoming scan.

An example hook looks like.

```python
__all__ = ["dranspose"]

from sardana.macroserver.macro import macro, Macro, Type, Optional
from tango import DeviceProxy
import json
import requests

class dranspose(Macro):
    """Notify a dranspose Pipeline of a scan"""
    param_def = [['url', Type.String, None, "url to the pipeline controller"]]

    def run(self, url):
        active_mnt_group = self.getEnv("ActiveMntGrp")

        # Device proxy to active measurement group
        active_mnt_group_dp = DeviceProxy(active_mnt_group)
        # Controllers on active measurement group
        mg_configuration = json.loads(active_mnt_group_dp.Configuration)
        controllers = mg_configuration["controllers"]

        self.output(f"dranspose triggermap to {url}")
        self.output("streams in measurement group:")
        streams = []
        for conf in controllers.values():
            for ch in conf["channels"].values():
                if ch["enabled"] is True:
                    self.output(ch["name"])
                    streams.append(ch["name"])
        
        if self._parent_macro is not None:
            # build triggermap depending on the type of scan
            if self._parent_macro._name == "burstscan":
                burst = -self._parent_macro.integ_time
                steps = self._parent_macro.nb_points
                # only create frames for detectors which have ingesters
                available_streams = ["andor3_balor", "andor3_zyla10", "andor3_zyla12", "pilatus"]
                mapping = {det:[
                    [{"constraint":i}] for i in range(burst*steps)]
                    for det in available_streams if det in streams}
                mapping["sardana"] = [[{"constraint":i}] if 
                                      i%burst==burst-1 else 
                                      None for i in range(burst*steps)]
                req = requests.post(f"{url}/api/v1/mapping", json=mapping)
                self.output(f"{req.status_code}, msg: {req.content}")
```

The hook is then enabled with

```bash
defgh "dranspose http://femtomax-pipeline-controller.daq.maxiv.lu.se" pre-scan
```