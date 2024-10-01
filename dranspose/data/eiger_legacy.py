from typing import Literal

from pydantic import BaseModel, TypeAdapter, ConfigDict


class EigerLegacy(BaseModel):
    pass


class EigerLegacyHeader(EigerLegacy):
    """
    Example:
        ``` py
        EigerLegacyHeader(
            htype='dheader-1.0'
            header_detail='all'
            series=6
            info={'auto_summation': True,
                  'beam_center_x': 0.0,
                  'beam_center_y': 0.0,
                  'bit_depth_image': 32,
                  'bit_depth_readout': 16,
                  'chi_increment': 0.0,
                  'chi_start': 0.0,
                  'compression': 'bslz4',
                  'count_time': 0.0099999,
                  'countrate_correction_applied': True,
                  'countrate_correction_count_cutoff': 83270,
                  'data_collection_date': '2024-03-12T15:29:58.142+01:00',
                  'description': 'Dectris EIGER2 CdTe 9M',
                  'detector_distance': 0.0,
                  'detector_number': 'E-18-0000',
                  'detector_readout_time': 1e-07,
                  'detector_translation': [0.0, 0.0, 0.0],
                  'eiger_fw_version': 'release-2022.1.1',
                  'element': '',
                  'flatfield_correction_applied': True,
                  'frame_count_time': 0.00499996,
                  'frame_period': 0.00499996,
                  'frame_time': 0.01,
                  'kappa_increment': 0.0,
                  'kappa_start': 0.0,
                  'nimages': 3,
                  'ntrigger': 1,
                  'number_of_excluded_pixels': 683998,
                  'omega_increment': 0.0,
                  'omega_start': 0.0,
                  'phi_increment': 0.0,
                  'phi_start': 0.0,
                  'photon_energy': 8041.0,
                  'pixel_mask_applied': True,
                  'roi_mode': '',
                  'sensor_material': 'CdTe',
                  'sensor_thickness': 0.00075,
                  'software_version': '1.8.0',
                  'threshold_energy': 4020.5,
                  'trigger_mode': 'ints',
                  'two_theta_increment': 0.0,
                  'two_theta_start': 0.0,
                  'virtual_pixel_correction_applied': True,
                  'wavelength': 1.5419002416764116,
                  'x_pixel_size': 7.5e-05,
                  'x_pixels_in_detector': 3108,
                  'y_pixel_size': 7.5e-05,
                  'y_pixels_in_detector': 3262}
            appendix={'collect_dict': {
                        'col_id': 128579,
                        'process_dir': '/data/.../process',
                        'target_beam_size_factor': 0,
                        'ssx_mode': 'test',
                        'exp_type': 'test',
                        'shape_id': '2DP1',
                        'col': 0,
                        'row': 0},
                      'dozor_dict': {
                        'x_pixels_in_detector': 3108,
                        'omega_increment': None,
                        'count_time': 0.0099999,
                        'detector_distance': 0.0,
                        'beam_center_y': 0.0,
                        'beam_center_x': 0.0,
                        'y_pixels_in_detector': 3262,
                        'wavelength': 1.541900241676412,
                        'countrate_correction_count_cutoff': 83270,
                        'x_pixel_size': 7.5e-05}
                      }
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["dheader-1.0"]


class EigerLegacyImage(EigerLegacy):
    """
    Example:
        ``` py
        EigerLegacyImage(
            htype='dimage-1.0',
            frame=0
            hash='815e4907ce3482ec3cc4d33a1145cd44'
            series=6
            data={'encoding': 'bs32-lz4<',
                  'htype': 'dimage_d-1.0',
                  'shape': [3108, 3262],
                  'size': 754672,
                  'type': 'uint32',
                  'buffer': b'\x00\x00\x00\x00\x02j\xca\xe0...'
            config={'htype': 'dconfig-1.0',
                    'real_time': 9999900,
                    'start_time': 103597359252740,
                    'stop_time': 103597369252640}
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["dimage-1.0"]
    frame: int


class EigerLegacyEnd(EigerLegacy):
    """
    Example:
        ``` py
        EigerLegacyEnd(
            htype='dseries_end-1.0'
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["dseries_end-1.0"]


EigerLegacyPacket = TypeAdapter(EigerLegacyHeader | EigerLegacyImage | EigerLegacyEnd)
"""
A union type for Eiger Legacy packets
"""
