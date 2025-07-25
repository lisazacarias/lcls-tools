import csv
import yaml
import os
from typing import Any, Union, List, Dict, Optional
import numpy as np
from lcls_tools.common.devices.yaml.metadata import (
    get_magnet_metadata,
    get_screen_metadata,
    get_wire_metadata,
    get_lblm_metadata,
    get_bpm_metadata,
    get_tcav_metadata,
)
from lcls_tools.common.devices.yaml.controls_information import (
    get_magnet_controls_information,
    get_screen_controls_information,
    get_wire_controls_information,
    get_lblm_controls_information,
    get_bpm_controls_information,
    get_tcav_controls_information,
)


class YAMLGenerator:
    def __init__(
        self,
        csv_location="./lcls_tools/common/devices/yaml/lcls_elements.csv",
        filter_location="./lcls_tools/common/devices/yaml/config/filter.yaml",
    ):
        self.csv_location = csv_location
        self.filter_location = filter_location
        if not os.path.isfile(csv_location):
            raise FileNotFoundError(f"Could not find {csv_location}")
        self._required_fields = [
            "Element",
            "Control System Name",
            "Area",
            "Keyword",
            "Beampath",
            "SumL (m)",
        ]
        self.elements = self._filter_elements_by_fields(self._required_fields)
        self._areas = self.extract_areas()
        self._beam_paths = self.extract_beampaths()

    def _filter_elements_by_fields(self, required_fields: List[str]) -> Dict[str, Any]:
        csv_reader = None
        with (
            open(self.csv_location, "r") as file_csv,
            open(self.filter_location, "r") as file_filter,
        ):
            # convert csv file into dictionary for filtering
            csv_reader = csv.DictReader(f=file_csv)
            filter_dict = yaml.safe_load(file_filter)

            def _is_filtered_row(element: dict):
                released = True
                for key in set(filter_dict.keys()) & set(element.keys()):
                    value = element[key]
                    for prefix in filter_dict[key]:
                        released &= not value.startswith(prefix)
                return released

            element_list = list(filter(_is_filtered_row, csv_reader))

            # make the elements from csv stripped out with only information we need
            def _is_required_field(pair: tuple):
                key, value = pair
                return key in required_fields

            # only store the required fields from lcls_elements, there are lots more!
            element_list = [
                dict(filter(_is_required_field, element.items()))
                for element in element_list
            ]

        if not element_list:
            raise RuntimeError(
                "Did not generate elements, please look at lcls_elements.csv."
            )
        return element_list

    def extract_areas(self) -> list:
        areas = []
        [
            areas.append(element["Area"])
            for element in self.elements
            if element["Area"] not in areas
        ]
        return areas

    @property
    def areas(self) -> list:
        return self._areas

    def extract_beampaths(self) -> list:
        beampaths = []
        [
            beampaths.append(beampath)
            for element in self.elements
            for beampath in element["Beampath"].split(",")
            if beampath not in beampaths and beampath != ""
        ]
        return beampaths

    @property
    def beam_paths(self) -> list:
        return self._beam_paths

    def _construct_information_from_element(
        self,
        element,
        pv_information: Optional[Dict[str, str]] = {},
        additional_metadata_fields: Dict[str, Any] = {},
        additional_control_fields: Dict[str, Any] = {},
    ):
        """
        Generates a dictionary with only the relevant information we want
        from the Dict that lcls_elements.csv is loaded into.
        """
        sum_l_meters = float(element["SumL (m)"]) if element["SumL (m)"] else None
        device_information = {
            "controls_information": {
                "control_name": element["Control System Name"],
                "PVs": pv_information,
            },
            "metadata": {
                "beam_path": [
                    item.strip() for item in element["Beampath"].split(",") if item
                ],
                "area": element["Area"],
                "type": element["Keyword"],
                "sum_l_meters": (
                    float(np.format_float_positional(sum_l_meters, precision=3))
                    if sum_l_meters is not None
                    else None
                ),
            },
        }
        [
            device_information["metadata"].update({field_name: field_value})
            for field_name, field_value in additional_metadata_fields.items()
        ]
        [
            device_information["controls_information"].update(
                {field_name: element[field_name]}
            )
            for field_name in additional_control_fields.items()
        ]
        return device_information

    def _construct_pv_list_from_control_system_name(
        self, name, search_with_handles: Optional[Dict[str, str]]
    ) -> Dict[str, str]:
        from meme import names

        if name == "":
            raise RuntimeError("No control system name provided for meme search.")
        # Use the control system name to get all PVs associated with device
        pv_dict = {}
        for search_term, handle in search_with_handles.items():
            field = str()
            if "." in search_term:
                search_term, field = search_term.split(".")
            # End of the PV name is implied in search_term
            try:
                pv_list = names.list_pvs(name + ":" + search_term, sort_by="z")
                # We expect to have ZERO or ONE result returned from meme
                if pv_list != list():
                    if len(pv_list) == 1:
                        # get the pv out of the results
                        pv = f"{pv_list[0]}.{field}" if field else pv_list[0]
                        if not handle:
                            # if the user has not provided their own handle then
                            # split by colon, grab the last part of the string as a handle
                            name_in_yaml = pv.split(":")[-1].lower()
                        else:
                            # user has provided their own handle.
                            name_in_yaml = handle
                        # add it to the dictionary of PVs
                        pv_dict[name_in_yaml] = pv
                    else:
                        raise RuntimeError(
                            f"Did not return unique PV search result from MEME, please check MEME {name}:{search_term}"
                        )
            except TimeoutError as toe:
                print(
                    f"Unable connect to MEME.name service when searching for {name + ':' + search_term}."
                )
                print(toe)
        return pv_dict

    def extract_devices(
        self,
        area: Union[str, List[str]],
        required_types=Optional[List[str]],
        pv_search_terms=Optional[List[str]],
        **kwargs_additional_constraints,
    ):
        if not isinstance(area, list):
            machine_areas = [area]
        else:
            machine_areas = area
        yaml_devices = {}
        # duplicate fields could cause issues, should take the set,
        # then convert back? does ordering matter?
        required_fields = self._required_fields + list(
            kwargs_additional_constraints.keys()
        )
        elements = self._filter_elements_by_fields(required_fields=required_fields)
        for _area in machine_areas:
            device_elements = [
                element
                for element in elements
                if (
                    element["Keyword"] in required_types
                    and element["Area"] == _area
                    and all(
                        element.get(key) == value
                        for key, value in kwargs_additional_constraints.items()
                    )
                )
            ]
        # Must have passed an area that does not exist or we don't have that device in this area!
        if len(device_elements) < 1:
            print(f"No devices of types {required_types} found in area {area}")
            return
        # Fill in the dict that will become the yaml file
        for device in device_elements:
            # We need a control-system-name
            if device["Control System Name"] != "":
                pv_info = None
                try:
                    # grab the pv information for this element using the search_list
                    pv_info = self._construct_pv_list_from_control_system_name(
                        name=device["Control System Name"],
                        search_with_handles=pv_search_terms,
                    )
                except RuntimeError as rte:
                    print(rte)
                # add device and information to the yaml-contents
                yaml_devices.update(
                    {
                        device["Element"]: self._construct_information_from_element(
                            device,
                            pv_information=pv_info,
                        )
                    }
                )
        return yaml_devices

    def add_to_device_metadata(
        self,
        device_data: Dict[str, Any],
        additional_metadata: Dict[str, Any] = {},
    ) -> Dict[str, Any]:
        for device in device_data.keys():
            try:
                device_data[device]["metadata"].update(additional_metadata[device])
            except KeyError:
                print("No additional metadata found for ", device)

        return device_data

    def add_to_device_controls_information(
        self,
        device_data: Dict[str, Any],
        additional_controls_information: Dict[str, Any] = {},
    ) -> Dict[str, Any]:
        for device in device_data.keys():
            try:
                device_data[device]["controls_information"].update(
                    additional_controls_information[device]
                )
            except KeyError:
                print("No additional controls information found for ", device)
        return device_data

    def add_extra_data_to_device(
        self,
        device_data: Dict[str, Any],
        additional_controls_information: Dict[str, Any] = {},
        additional_metadata: Dict[str, Any] = {},
    ) -> Dict[str, Any]:
        complete_device_data = {}
        complete_device_data.update(
            self.add_to_device_metadata(
                device_data=device_data,
                additional_metadata=additional_metadata,
            ),
        )
        complete_device_data.update(
            self.add_to_device_controls_information(
                device_data=complete_device_data,
                additional_controls_information=additional_controls_information,
            ),
        )
        return complete_device_data

    def extract_magnets(self, area: Union[str, List[str]] = "GUNB") -> dict:
        required_magnet_types = ["SOLE", "QUAD", "XCOR", "YCOR", "BEND"]
        # PV suffix as the key, the name we want to store it as in yaml file as the value
        # None implies that we are happen using the PV suffix (lowercase) as the name in yaml
        possible_magnet_pvs = {
            "BACT": None,
            "BCTRL": None,
            "BCON": None,
            "BDES": None,
            "CTRL": None,
            "BMIN": None,
            "BMAX": None,
        }
        # should be structured {MAD-NAME : {field_name : value, field_name_2 : value}, ... }

        basic_magnet_data = self.extract_devices(
            area=area,
            required_types=required_magnet_types,
            pv_search_terms=possible_magnet_pvs,
        )

        if basic_magnet_data:
            magnet_names = [key for key in basic_magnet_data.keys()]
            additional_metadata_data = get_magnet_metadata(
                magnet_names, self.extract_metadata_by_device_names
            )
            # should be structured {MAD-NAME : {field_name : value, field_name_2 : value}, ... }
            additional_controls_data = get_magnet_controls_information()

            complete_magnet_data = self.add_extra_data_to_device(
                device_data=basic_magnet_data,
                additional_controls_information=additional_controls_data,
                additional_metadata=additional_metadata_data,
            )

            return complete_magnet_data
        else:
            return {}

    def extract_screens(self, area: Union[str, List[str]] = ["HTR"]):
        required_screen_types = ["PROF"]
        # PV suffix as the key, the name we want to store it as in yaml file as the value
        # None implies that we are happen using the PV suffix (lowercase) as the name in yaml
        possible_screen_pvs = {
            "IMAGE": "image",
            "Image:ArrayData": "image",
            "RESOLUTION": None,
            "Image:ArraySize0_RBV": "n_row",
            "Image:ArraySize1_RBV": "n_col",
            "N_OF_COL": "n_col",
            "N_OF_ROW": "n_row",
            "N_OF_BITS": "n_bits",
            "SYS_TYPE": "sys_type",
            "FRAME_RATE": "ref_rate_vme",
            "ArrayRate_RBV": "ref_rate",
            "PNEUMATIC": "target_control",
            "TGT_STS": "target_status",
            "FLT1_STS": "filter_1_status",
            "FLT1_CTRL": "filter_1_control",
            "FLT2_STS": "filter_2_status",
            "FLT2_CTRL": "filter_2_control",
            "TGT_LAMP_PWR": "lamp_power",
            "X_ORIENT": "orient_x",
            "Y_ORIENT": "orient_y",
        }
        basic_screen_data = self.extract_devices(
            area=area,
            required_types=required_screen_types,
            pv_search_terms=possible_screen_pvs,
        )
        if basic_screen_data:
            # should be structured {MAD-NAME : {field_name : value, field_name_2 : value}, ... }
            additional_metadata_data = get_screen_metadata(basic_screen_data)
            additional_controls_data = get_screen_controls_information(
                basic_screen_data
            )
            complete_screen_data = self.add_extra_data_to_device(
                device_data=basic_screen_data,
                additional_controls_information=additional_controls_data,
                additional_metadata=additional_metadata_data,
            )
            return complete_screen_data
        else:
            return {}

    def extract_wires(self, area: Union[str, List[str]] = ["HTR"]):
        required_wire_types = ["WIRE"]
        # PV suffix as the key, the name we want to store it as in yaml file as the value
        # None implies that we are happen using the PV suffix (lowercase) as the name in yaml
        possible_wire_pvs = {
            "MOTR.STOP": "abort_scan",
            "BEAMRATE": "beam_rate",
            "MOTR_ENABLED_STS": "enabled",
            "MOTR_HOMED_STS": "homed",
            "MOTR_INIT": "initialize",
            "MOTR_INIT_STS": "initialize_status",
            "MOTR": "motor",
            "MOTR.RBV": "motor_rbv",
            "MOTR_RETRACT": "retract",
            "SCANPULSES": "scan_pulses",
            "MOTR.VELO": "speed",
            "MOTR.VMAX": "speed_max",
            "MOTR.VBAS": "speed_min",
            "STARTSCAN": "start_scan",
            "TEMP": "temperature",
            "MOTR_TIMEOUTEN": "timeout",
            "USEUWIRE": "use_u_wire",
            "USEXWIRE": "use_x_wire",
            "USEYWIRE": "use_y_wire",
            "UWIRESIZE": "u_size",
            "UWIREINNER": "u_wire_inner",
            "UWIREOUTER": "u_wire_outer",
            "XWIRESIZE": "x_size",
            "XWIREINNER": "x_wire_inner",
            "XWIREOUTER": "x_wire_outer",
            "YWIRESIZE": "y_size",
            "YWIREINNER": "y_wire_inner",
            "YWIREOUTER": "y_wire_outer",
        }
        # should be structured {MAD-NAME : {field_name : value, field_name_2 : value}, ... }
        additional_metadata_data = get_wire_metadata()
        # should be structured {MAD-NAME : {field_name : value, field_name_2 : value}, ... }
        additional_controls_data = get_wire_controls_information()
        basic_wire_data = self.extract_devices(
            area=area,
            required_types=required_wire_types,
            pv_search_terms=possible_wire_pvs,
        )
        if basic_wire_data:
            complete_wire_data = self.add_extra_data_to_device(
                device_data=basic_wire_data,
                additional_controls_information=additional_controls_data,
                additional_metadata=additional_metadata_data,
            )
            return complete_wire_data
        else:
            return {}

    def extract_lblms(self, area: Union[str, List[str]] = ["HTR"]):
        required_lblm_types = ["LBLM"]
        # PV suffix as the key, the name we want to store it as in yaml file as the value
        # None implies that we are happen using the PV suffix (lowercase) as the name in yaml
        possible_lblm_pvs = {
            "GATED_INTEGRAL": "gated_integral",
            "I0_LOSS": "i0_loss",
            "FAST_AMP_GAIN": "gain",
            "FAST_AMP_BYP": "bypass",
        }
        # should be structured {MAD-NAME : {field_name : value, field_name_2 : value}, ... }
        additional_metadata_data = get_lblm_metadata()
        # should be structured {MAD-NAME : {field_name : value, field_name_2 : value}, ... }
        additional_controls_data = get_lblm_controls_information()
        basic_lblm_data = self.extract_devices(
            area=area,
            required_types=required_lblm_types,
            pv_search_terms=possible_lblm_pvs,
        )
        if basic_lblm_data:
            complete_lblm_data = self.add_extra_data_to_device(
                device_data=basic_lblm_data,
                additional_controls_information=additional_controls_data,
                additional_metadata=additional_metadata_data,
            )
            return complete_lblm_data
        else:
            return {}

    def extract_bpms(self, area: Union[str, List[str]] = ["HTR"]):
        required_bpm_types = ["BPM"]
        # PV suffix as the key, the name we want to store it as in yaml file as the value
        # None implies that we are happen using the PV suffix (lowercase) as the name in yaml
        possible_bpm_pvs = {
            "X": "x",
            "Y": "y",
            "TMIT": "tmit",
        }
        # should be structured {MAD-NAME : {field_name : value, field_name_2 : value}, ... }
        additional_metadata_data = get_bpm_metadata()
        # should be structured {MAD-NAME : {field_name : value, field_name_2 : value}, ... }
        additional_controls_data = get_bpm_controls_information()
        basic_bpm_data = self.extract_devices(
            area=area,
            required_types=required_bpm_types,
            pv_search_terms=possible_bpm_pvs,
        )
        if basic_bpm_data:
            complete_bpm_data = self.add_extra_data_to_device(
                device_data=basic_bpm_data,
                additional_controls_information=additional_controls_data,
                additional_metadata=additional_metadata_data,
            )
            return complete_bpm_data
        else:
            return {}

    def extract_tcavs(self, area: Union[str, List[str]] = ["DIAG0"]) -> dict:
        required_tcav_types = ["LCAV"]
        additional_filter_constraints = {"Engineering Name": "TRANS_DEFL"}
        # add pvs we care about
        possible_tcav_pvs = {
            "AREQ": "amplitude",
            "PREQ": "phase",
            "RF_ENABLE": "rf_enable",
            "AFBENB": "amplitude_fbenb",
            "PFBENB": "phase_fbenb",
            "AFBST": "amplitude_fbst",
            "PFBST": "phase_fbst",
            "MODECFG": "mode_config",
            "PACT_AVGNT": "phase_avgnt",
            "AMPL_W0CH0": "amplitude_wocho",
        }

        basic_tcav_data = self.extract_devices(
            area=area,
            required_types=required_tcav_types,
            pv_search_terms=possible_tcav_pvs,
            **additional_filter_constraints,
        )
        if basic_tcav_data:
            tcav_names = [key for key in basic_tcav_data.keys()]
            additional_metadata_data = get_tcav_metadata(
                tcav_names, self.extract_metadata_by_device_names
            )
            additional_controls_data = get_tcav_controls_information()
            complete_tcav_data = self.add_extra_data_to_device(
                device_data=basic_tcav_data,
                additional_controls_information=additional_controls_data,
                additional_metadata=additional_metadata_data,
            )
            return complete_tcav_data
        else:
            return {}

    def extract_metadata_by_device_names(
        self, device_names=Optional[List[str]], required_fields=Optional[List[str]]
    ):
        # TODO: try not to call filter elements so many times as it parses csv
        if required_fields:
            elements = self._filter_elements_by_fields(required_fields=required_fields)
        else:
            elements = self._filter_elements_by_fields(
                required_fields=self._required_fields
            )

        device_elements = {
            element["Element"]: {
                required_field: element[required_field]
                for required_field in required_fields
                if "Element" not in required_field
            }
            for element in elements
            if element["Element"] in device_names
        }

        return device_elements
