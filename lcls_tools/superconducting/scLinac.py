################################################################################
# Utility classes for superconducting linac
# NOTE: For some reason, using python 3 style type annotations causes circular
#       import issues, so leaving as python 2 style for now
################################################################################
from time import sleep
from typing import Dict, List, Type

from epics import PV
from numpy import sign

import lcls_tools.superconducting.scLinacUtils as utils


class SSA:
    def __init__(self, cavity):
        # type: (Cavity) -> None
        self.cavity: Cavity = cavity
        self.pvPrefix = self.cavity.pvPrefix + "SSA:"

        self.ssaStatusPV: PV = PV(self.pvPrefix + "StatusMsg")
        self.ssaTurnOnPV: PV = PV(self.pvPrefix + "PowerOn")
        self.ssaTurnOffPV: PV = PV(self.pvPrefix + "PowerOff")

        self.ssaCalibrationStartPV: PV = PV(self.pvPrefix + "CALSTRT")
        self.ssaCalibrationStatusPV: PV = PV(self.pvPrefix + "CALSTS")

        self.currentSSASlopePV: PV = PV(self.pvPrefix + "SLOPE")
        self.measuredSSASlopePV: PV = PV(self.pvPrefix + "SLOPE_NEW")

    def turnOn(self):
        self.setPowerState(True)

    def turnOff(self):
        self.setPowerState(False)

    def setPowerState(self, turnOn: bool):
        print("\nSetting SSA power...")

        if turnOn and self.ssaStatusPV != utils.SSA_STATUS_ON_VALUE:
            self.ssaTurnOnPV.put(1)
            sleep(7)
            if self.ssaStatusPV != utils.SSA_STATUS_ON_VALUE:
                raise utils.SSAPowerError("Unable to turn on SSA")
        else:
            if self.ssaStatusPV == utils.SSA_STATUS_ON_VALUE:
                self.ssaTurnOffPV.put(1)
                sleep(1)
                if self.ssaStatusPV == utils.SSA_STATUS_ON_VALUE:
                    raise utils.SSAPowerError("Unable to turn off SSA")

        print("SSA power set\n")

    def runCalibration(self):
        """
        Runs the SSA through its range and finds the slope that describes
        the relationship between SSA drive signal and output power
        :return:
        """
        self.setPowerState(True)
        utils.runCalibration(startPV=self.ssaCalibrationStartPV,
                             statusPV=self.ssaCalibrationStatusPV,
                             exception=utils.SSACalibrationError)

        utils.pushAndSaveCalibrationChange(measuredPV=self.measuredSSASlopePV,
                                           currentPV=self.currentSSASlopePV,
                                           lowerLimit=utils.SSA_SLOPE_LOWER_LIMIT,
                                           upperLimit=utils.SSA_SLOPE_UPPER_LIMIT,
                                           pushPV=self.cavity.pushSSASlopePV,
                                           savePV=self.cavity.saveSSASlopePV,
                                           exception=utils.SSACalibrationError)


class Heater:
    def __init__(self, cavity):
        # type: (Cavity) -> None
        self.cavity = cavity
        self.pvPrefix = "CHTR:CM{cm}:1{cav}55:HV:".format(cm=self.cavity.cryomodule.name,
                                                          cav=self.cavity.number)
        self.powerDesPV = PV(self.pvPrefix + "POWER_SETPT")
        self.powerActPV = PV(self.pvPrefix + "POWER")


class StepperTuner:
    def __init__(self, cavity):
        # type (Cavity) -> None

        self.cavity: Cavity = cavity
        self.pvPrefix: str = self.cavity.pvPrefix + "STEP:"

        self.move_pos_pv: PV = PV(self.pvPrefix + "MOV_REQ_POS")
        self.move_neg_pv: PV = PV(self.pvPrefix + "MOV_REQ_NEG")
        self.abort_pv: PV = PV(self.pvPrefix + "ABORT_REQ")
        self.step_des_pv: PV = PV(self.pvPrefix + "NSTEPS")
        self.max_steps_pv: PV = PV(self.pvPrefix + "NSTEPS.DRVH")
        self.speed_pv: PV = PV(self.pvPrefix + "VELO")
        self.step_tot_pv: PV = PV(self.pvPrefix + "REG_TOTABS")
        self.step_signed_pv: PV = PV(self.pvPrefix + "REG_TOTSGN")
        self.reset_tot_pv: PV = PV(self.pvPrefix + "TOTABS_RESET")
        self.reset_signed_pv: PV = PV(self.pvPrefix + "TOTSGN_RESET")
        self.steps_cold_landing_pv: PV = PV(self.pvPrefix + "NSTEPS_COLD")
        self.push_signed_cold_pv: PV = PV(self.pvPrefix + "PUSH_NSTEPS_COLD.PROC")
        self.push_signed_park_pv: PV = PV(self.pvPrefix + "PUSH_NSTEPS_PARK.PROC")
        self.motor_moving_pv: PV = PV(self.pvPrefix + "STAT_MOV")
        self.motor_done_pv: PV = PV(self.pvPrefix + "STAT_DONE")

        self.step_tot_pv.add_callback(self.checkTemp)

    def checkTemp(self):
        if self.cavity.stepper_temp_PV.value >= utils.STEPPER_TEMP_LIMIT:
            self.abort_pv.put(1)

    def restoreDefaults(self):
        self.max_steps_pv.put(utils.DEFAULT_STEPPER_MAX_STEPS)
        self.speed_pv.put(utils.DEFAULT_STEPPER_SPEED)

    def move(self, numSteps: int, maxSteps: int = utils.DEFAULT_STEPPER_MAX_STEPS,
             speed: int = utils.DEFAULT_STEPPER_SPEED, changeLimits: bool = True):
        """
        :param numSteps: positive for increasing cavity length, negative for decreasing
        :param maxSteps: the maximum number of steps allowed at once
        :param speed: the speed of the motor in steps/second
        :param changeLimits: whether or not to change the speed and steps
        :return:
        """

        if changeLimits:
            # on the off chance that someone tries to write a negative maximum
            self.max_steps_pv.put(abs(maxSteps))

            # make sure that we don't exceed the speed limit as defined by the tuner experts
            self.speed_pv.put(speed if speed < utils.MAX_STEPPER_SPEED
                              else utils.MAX_STEPPER_SPEED)

        if abs(numSteps) <= maxSteps:
            self.step_des_pv.put(numSteps)
            self.issueMoveCommand(numSteps)
            self.restoreDefaults()
        else:
            self.step_des_pv.put(maxSteps)
            self.issueMoveCommand(numSteps)

            self.move(numSteps - (sign(numSteps) * maxSteps), maxSteps, speed,
                      False)

    def issueMoveCommand(self, numSteps):

        # this is necessary because the tuners for the HLs move the other direction
        if self.cavity.cryomodule.isHarmonicLinearizer:
            numSteps *= -1

        if sign(numSteps) == 1:
            self.move_pos_pv.put(1)
        else:
            self.move_neg_pv.put(1)

        while self.motor_moving_pv.value == 1:
            sleep(1)

        if self.motor_done_pv.value != 1:
            raise utils.StepperError("Motor not in expected state")


class Cavity:
    def __init__(self, cavityNum, rackObject, ssaClass=SSA):
        # type: (int, Rack, Type[SSA]) -> None
        """
        Parameters
        ----------
        cavityNum: int cavity number i.e. 1 - 8
        rackObject: the rack object the cavities belong to
        """

        self.number = cavityNum
        self.rack: Rack = rackObject
        self.cryomodule = self.rack.cryomodule
        self.linac = self.cryomodule.linac

        if self.cryomodule.isHarmonicLinearizer:
            self.length = 0.346
            self.frequency = 3.9e9
        else:
            self.length = 1.038
            self.frequency = 1.3e9

        self.pvPrefix = "ACCL:{LINAC}:{CRYOMODULE}{CAVITY}0:".format(LINAC=self.linac.name,
                                                                     CRYOMODULE=self.cryomodule.name,
                                                                     CAVITY=self.number)

        self.ctePrefix = "CTE:CM{cm}:1{cav}".format(cm=self.cryomodule.name,
                                                    cav=self.number)

        self.ssa = ssaClass(self)
        self.heater = Heater(self)
        self.steppertuner = StepperTuner(self)

        self.pushSSASlopePV: PV = PV(self.pvPrefix + "PUSH_SSA_SLOPE.PROC")
        self.saveSSASlopePV: PV = PV(self.pvPrefix + "SAVE_SSA_SLOPE.PROC")
        self.interlockResetPV: PV = PV(self.pvPrefix + "INTLK_RESET_ALL")

        self.drivelevelPV: PV = PV(self.pvPrefix + "SEL_ASET")

        self.cavityCalibrationStartPV: PV = PV(self.pvPrefix + "PROBECALSTRT")
        self.cavityCalibrationStatusPV: PV = PV(self.pvPrefix + "PROBECALSTS")

        self.currentQLoadedPV: PV = PV(self.pvPrefix + "QLOADED")
        self.measuredQLoadedPV: PV = PV(self.pvPrefix + "QLOADED_NEW")
        self.pushQLoadedPV: PV = PV(self.pvPrefix + "PUSH_QLOADED.PROC")
        self.saveQLoadedPV: PV = PV(self.pvPrefix + "SAVE_QLOADED.PROC")

        self.currentCavityScalePV: PV = PV(self.pvPrefix + "CAV:SCALER_SEL.B")
        self.measuredCavityScalePV: PV = PV(self.pvPrefix + "CAV:CAL_SCALEB_NEW")
        self.pushCavityScalePV: PV = PV(self.pvPrefix + "PUSH_CAV_SCALE.PROC")
        self.saveCavityScalePV: PV = PV(self.pvPrefix + "SAVE_CAV_SCALE.PROC")

        self.selAmplitudeDesPV: PV = PV(self.pvPrefix + "ADES")
        self.selAmplitudeActPV: PV = PV(self.pvPrefix + "AACTMEAN")

        self.rfModeCtrlPV: PV = PV(self.pvPrefix + "RFMODECTRL")
        self.rfModePV: PV = PV(self.pvPrefix + "RFMODE")

        self.rfStatePV: PV = PV(self.pvPrefix + "RFSTATE")
        self.rfControlPV: PV = PV(self.pvPrefix + "RFCTRL")

        self.pulseGoButtonPV: PV = PV(self.pvPrefix + "PULSE_DIFF_SUM")
        self.pulseStatusPV = PV(self.pvPrefix + "PULSE_STATUS")
        self.pulseOnTimePV: PV = PV(self.pvPrefix + "PULSE_ONTIME")

        self.revWaveformPV: PV = PV(self.pvPrefix + "REV:AWF")
        self.fwdWaveformPV: PV = PV(self.pvPrefix + "FWD:AWF")
        self.cavWaveformPV: PV = PV(self.pvPrefix + "CAV:AWF")

        self.stepper_temp_PV: PV = PV(self.pvPrefix + "STEPTEMP")

    def checkAndSetOnTime(self):
        """
        In pulsed mode the cavity has a duty cycle determined by the on time and
        off time. We want the on time to be 70 ms or else the various cavity
        parameters calculated from the waveform (e.g. the RF gradient) won't be
        accurate.
        :return:
        """
        print("Checking RF Pulse On Time...")
        if self.pulseOnTimePV.value != utils.NOMINAL_PULSED_ONTIME:
            print("Setting RF Pulse On Time to {ontime} ms".format(ontime=utils.NOMINAL_PULSED_ONTIME))
            self.pulseOnTimePV.put(utils.NOMINAL_PULSED_ONTIME)
            self.pushGoButton()

    def pushGoButton(self):
        """
        Many of the changes made to a cavity don't actually take effect until the
        go button is pressed
        :return:
        """
        self.pulseGoButtonPV.put(1)
        while self.pulseStatusPV.value < 2:
            sleep(1)
        if self.pulseStatusPV.value > 2:
            raise utils.PulseError("Unable to pulse cavity")

    def turnOn(self):
        self.setPowerState(True)

    def turnOff(self):
        self.setPowerState(False)

    def setPowerState(self, turnOn: bool):
        """
        Turn the cavity on or off
        :param turnOn:
        :return:
        """
        desiredState = (1 if turnOn else 0)

        if self.rfStatePV.value != desiredState:
            print("\nSetting RF State...")
            self.rfControlPV.put(desiredState)

        print("RF state set\n")

    def runCalibration(self, loadedQLowerlimit=utils.LOADED_Q_LOWER_LIMIT,
                       loadedQUpperlimit=utils.LOADED_Q_UPPER_LIMIT):
        """
        Calibrates the cavity's RF probe so that the amplitude readback will be
        accurate. Also measures the loaded Q (quality factor) of the cavity power
        coupler
        :return:
        """
        self.interlockResetPV.put(1)
        sleep(2)

        self.drivelevelPV.put(15)

        utils.runCalibration(startPV=self.cavityCalibrationStartPV,
                             statusPV=self.cavityCalibrationStatusPV,
                             exception=utils.CavityQLoadedCalibrationError)

        utils.pushAndSaveCalibrationChange(measuredPV=self.measuredQLoadedPV,
                                           currentPV=self.currentQLoadedPV,
                                           lowerLimit=loadedQLowerlimit,
                                           upperLimit=loadedQUpperlimit,
                                           pushPV=self.pushQLoadedPV,
                                           savePV=self.saveQLoadedPV,
                                           exception=utils.CavityQLoadedCalibrationError)

        utils.pushAndSaveCalibrationChange(measuredPV=self.measuredCavityScalePV,
                                           currentPV=self.currentCavityScalePV,
                                           lowerLimit=utils.CAVITY_SCALE_LOWER_LIMIT,
                                           upperLimit=utils.CAVITY_SCALE_UPPER_LIMIT,
                                           pushPV=self.pushCavityScalePV,
                                           savePV=self.saveCavityScalePV,
                                           exception=utils.CavityScaleFactorCalibrationError)


class Magnet:
    def __init__(self, magnettype, cryomodule):
        # type: (str, Cryomodule) -> None
        self.pvprefix = "{magnettype}:{linac}:{cm}85:".format(magnettype=magnettype,
                                                              linac=cryomodule.linac.name,
                                                              cm=cryomodule.name)
        self.name = magnettype
        self.cryomodule: Cryomodule = cryomodule
        self.bdesPV: PV = PV(self.pvprefix + 'BDES')
        self.controlPV: PV = PV(self.pvprefix + 'CTRL')
        self.interlockPV: PV = PV(self.pvprefix + 'INTLKSUMY')
        self.ps_statusPV: PV = PV(self.pvprefix + 'STATE')
        self.bactPV: PV = PV(self.pvprefix + 'BACT')
        self.iactPV: PV = PV(self.pvprefix + 'IACT')
        # changing IDES immediately perturbs
        self.idesPV: PV = PV(self.pvprefix + 'IDES')

    @property
    def bdes(self):
        return self.bdesPV.value

    @bdes.setter
    def bdes(self, value):
        self.bdesPV.put(value)
        self.controlPV.put(utils.MAGNET_TRIM_VALUE)

    def reset(self):
        self.controlPV.put(utils.MAGNET_RESET_VALUE)

    def turnOn(self):
        self.controlPV.put(utils.MAGNET_ON_VALUE)

    def turnOff(self):
        self.controlPV.put(utils.MAGNET_OFF_VALUE)

    def degauss(self):
        self.controlPV.put(utils.MAGNET_DEGAUSS_VALUE)


class Rack:
    def __init__(self, rackName, cryoObject, cavityClass=Cavity, ssaClass=SSA):
        # type: (str, Cryomodule, Type[Cavity], Type[SSA]) -> None
        """
        Parameters
        ----------
        rackName: str name of rack (always either "A" or "B")
        cryoObject: the cryomodule object this rack belongs to
        cavityClass: cavity object
        """

        self.cryomodule = cryoObject
        self.rackName = rackName
        self.cavities = {}
        self.pvPrefix = self.cryomodule.pvPrefix + "RACK{RACK}:".format(RACK=self.rackName)

        if rackName == "A":
            # rack A always has cavities 1 - 4
            for cavityNum in range(1, 5):
                self.cavities[cavityNum] = cavityClass(cavityNum=cavityNum,
                                                       rackObject=self,
                                                       ssaClass=ssaClass)

        elif rackName == "B":
            # rack B always has cavities 5 - 8
            for cavityNum in range(5, 9):
                self.cavities[cavityNum] = cavityClass(cavityNum=cavityNum,
                                                       rackObject=self,
                                                       ssaClass=ssaClass)

        else:
            raise Exception("Bad rack name")


class Cryomodule:

    def __init__(self, cryoName, linacObject, cavityClass=Cavity,
                 magnetClass=Magnet, rackClass=Rack, isHarmonicLinearizer=False,
                 ssaClass=SSA):
        # type: (str, Linac, Type[Cavity], Type[Magnet], Type[Rack], bool, Type[SSA]) -> None
        """
        Parameters
        ----------
        cryoName: str name of Cryomodule i.e. "02", "03", "H1", "H2"
        linacObject: the linac object this cryomodule belongs to i.e. CM02 is in linac L1B
        cavityClass: cavity object
        """

        self.name: str = cryoName
        self.linac: Linac = linacObject
        self.isHarmonicLinearizer = isHarmonicLinearizer

        if not isHarmonicLinearizer:
            self.quad: Magnet = magnetClass("QUAD", self)
            self.xcor: Magnet = magnetClass("XCOR", self)
            self.ycor: Magnet = magnetClass("YCOR", self)

        self.pvPrefix = "ACCL:{LINAC}:{CRYOMODULE}00:".format(LINAC=self.linac.name,
                                                              CRYOMODULE=self.name)
        self.ctePrefix = "CTE:CM{cm}:".format(cm=self.name)
        self.cvtPrefix = "CVT:CM{cm}:".format(cm=self.name)
        self.cpvPrefix = "CPV:CM{cm}:".format(cm=self.name)
        self.jtPrefix = "CLIC:CM{cm}:3001:PVJT:".format(cm=self.name)

        self.dsLevelPV: PV = PV("CLL:CM{cm}:2301:DS:LVL".format(cm=self.name))
        self.usLevelPV: PV = PV("CLL:CM{cm}:2601:US:LVL".format(cm=self.name))
        self.dsPressurePV: PV = PV("CPT:CM{cm}:2303:DS:PRESS".format(cm=self.name))
        self.jtValveRdbkPV: PV = PV(self.jtPrefix + "ORBV")

        self.racks = {"A": rackClass(rackName="A", cryoObject=self,
                                     cavityClass=cavityClass,
                                     ssaClass=ssaClass),
                      "B": rackClass(rackName="B", cryoObject=self,
                                     cavityClass=cavityClass,
                                     ssaClass=ssaClass)}

        self.cavities: Dict[int, cavityClass] = {}
        self.cavities.update(self.racks["A"].cavities)
        self.cavities.update(self.racks["B"].cavities)

        if isHarmonicLinearizer:
            # two cavities share one SSA, this is the mapping
            cavity_ssa_pairs = [(1, 5), (2, 6), (3, 7), (4, 8)]

            for (leader, follower) in cavity_ssa_pairs:
                self.cavities[follower].ssa = self.cavities[leader].ssa
            self.couplerVacuumPVs: List[PV] = [PV(self.linac.vacuumPrefix + '{cm}09:COMBO_P'.format(cm=self.name)),
                                               PV(self.linac.vacuumPrefix + '{cm}19:COMBO_P'.format(cm=self.name))]
        else:
            self.couplerVacuumPVs: List[PV] = [PV(self.linac.vacuumPrefix + '{cm}14:COMBO_P'.format(cm=self.name))]

        self.vacuumPVs: List[str] = [pv.pvname for pv in (self.couplerVacuumPVs
                                                          + self.linac.beamlineVacuumPVs
                                                          + self.linac.insulatingVacuumPVs)]


class Linac:
    def __init__(self, linacName, beamlineVacuumInfixes, insulatingVacuumCryomodules):
        # type: (str, List[str], List[str]) -> None
        """
        Parameters
        ----------
        linacName: str name of Linac i.e. "L0B", "L1B", "L2B", "L3B"
        """

        self.name = linacName
        self.cryomodules: Dict[str, Cryomodule] = {}
        self.vacuumPrefix = 'VGXX:{linac}:'.format(linac=self.name)

        self.beamlineVacuumPVs = [PV(self.vacuumPrefix
                                     + '{infix}:COMBO_P'.format(infix=infix))
                                  for infix in beamlineVacuumInfixes]
        self.insulatingVacuumPVs = [PV(self.vacuumPrefix
                                       + '{cm}96:COMBO_P'.format(cm=cm))
                                    for cm in insulatingVacuumCryomodules]

    def addCryomodules(self, cryomoduleStringList, cryomoduleClass=Cryomodule,
                       cavityClass=Cavity, rackClass=Rack,
                       magnetClass=Magnet, isHarmonicLinearizer=False,
                       ssaClass=SSA):
        # type: (List[str], Type[Cryomodule], Type[Cavity], Type[Rack], Type[Magnet], bool, Type[SSA]) -> None

        for cryomoduleString in cryomoduleStringList:
            self.addCryomodule(cryomoduleName=cryomoduleString,
                               cryomoduleClass=cryomoduleClass,
                               cavityClass=cavityClass, rackClass=rackClass,
                               magnetClass=magnetClass,
                               isHarmonicLinearizer=isHarmonicLinearizer,
                               ssaClass=ssaClass)

    def addCryomodule(self, cryomoduleName, cryomoduleClass=Cryomodule,
                      cavityClass=Cavity, rackClass=Rack, magnetClass=Magnet,
                      isHarmonicLinearizer=False, ssaClass=SSA):
        # type: (str, Type[Cryomodule], Type[Cavity], Type[Rack], Type[Magnet], bool, Type[SSA]) -> None
        self.cryomodules[cryomoduleName] = cryomoduleClass(cryoName=cryomoduleName,
                                                           linacObject=self,
                                                           cavityClass=cavityClass,
                                                           rackClass=rackClass,
                                                           magnetClass=magnetClass,
                                                           isHarmonicLinearizer=isHarmonicLinearizer,
                                                           ssaClass=ssaClass)


# Global list of superconducting linac objects
L0B = ["01"]
L1B = ["02", "03"]
L1BHL = ["H1", "H2"]
L2B = ["04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15"]
L3B = ["16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27",
       "28", "29", "30", "31", "32", "33", "34", "35"]

LINAC_TUPLES = [("L0B", L0B), ("L1B", L1B), ("L2B", L2B), ("L3B", L3B)]

BEAMLINEVACUUM_INFIXES = [['0198'], ['0202', 'H292'], ['0402', '1592'], ['1602', '2594', '2598', '3592']]
INSULATINGVACUUM_CRYOMODULES = [['01'], ['02', 'H1'], ['04', '06', '08', '10', '12', '14'],
                                ['16', '18', '20', '22', '24', '27', '29', '31', '33', '34']]


def make_lcls_cryomodules(cryomoduleClass: Type[Cryomodule] = Cryomodule,
                          magnetClass: Type[Magnet] = Magnet,
                          rackClass: Type[Rack] = Rack,
                          cavityClass: Type[Cavity] = Cavity,
                          ssaClass: Type[SSA] = SSA) -> Dict[str, Cryomodule]:
    cryomoduleObjects: Dict[str, Cryomodule] = {}
    linacObjects: List[Linac] = []

    for idx, (name, cryomoduleList) in enumerate(LINAC_TUPLES):
        linac = Linac(name, beamlineVacuumInfixes=BEAMLINEVACUUM_INFIXES[idx],
                      insulatingVacuumCryomodules=INSULATINGVACUUM_CRYOMODULES[idx])
        linac.addCryomodules(cryomoduleStringList=cryomoduleList,
                             cryomoduleClass=cryomoduleClass,
                             cavityClass=cavityClass,
                             rackClass=rackClass,
                             magnetClass=magnetClass, ssaClass=ssaClass)
        linacObjects.append(linac)
        cryomoduleObjects.update(linac.cryomodules)

    linacObjects[1].addCryomodules(cryomoduleStringList=L1BHL,
                                   cryomoduleClass=cryomoduleClass,
                                   isHarmonicLinearizer=True,
                                   cavityClass=cavityClass,
                                   rackClass=rackClass,
                                   magnetClass=magnetClass, ssaClass=ssaClass)
    cryomoduleObjects.update(linacObjects[1].cryomodules)
    return cryomoduleObjects


CRYOMODULE_OBJECTS = make_lcls_cryomodules()
