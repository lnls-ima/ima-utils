# -*- coding: utf-8 -*-
"""
Created on 21/05/2014
Versão 1.0
@author: James Citadini
"""

import time as _time

from . import utils as _utils


class PDI5150Commands(object):
    """Commands of PDI5150 Integrator."""

    def __init__(self):
        """Load commands."""
        self.search_index = 'IND,'  # Procura de indice do integrador
        self.start = 'RUN'  # Inicia coleta com o integrador
        self.stop = 'BRK'  # Para coleta com o integrador
        self.read_status = 'STB,'  # Le status do integrador
        self.enquiry = 'ENQ'  # Busca Resultados do integrador
        self.channel = 'CHA,'  # Escolha de Canal
        self.gain = 'SGA,*,'  # Configura ganho integrador
        self.clear_over_range = 'CVR,*'  # Limpa Saturacao
        self.trigger_source_encoder = 'TRS,E,'  # Tipo de Trigger
        self.trigger_sequence = 'TRI,'  # Sequencia Trigger
        self.immediate_reading = 'IMD,1'  # Habilita leitura durante medida
        self.cumulative = 'CUM,0'  # Configura Dados para serem armazenados
        self.reset_counter = 'ZCT'  # Zerar contador de pulsos
        self.end_of_data = 'EOD'  # End of Data
        self.synchronization = 'SYN,1'  # Sincroniza
        self.offset_mode = 'ADJ,'  # Curto Integrador
        self.read_counter = 'RCT'  # Leitura Pulso Encoder


def PDI5150_factory(baseclass):
    """Create PDI5150 Integrator class."""
    class PDI5150(baseclass):
        """PDI5150 Integrator."""

        def __init__(self, log=False):
            """Initiaze variables and prepare log.

            Args:
                log (bool): True to use event logging, False otherwise.
            """
            self.commands = PDI5150Commands()
            self.conversion_factor = 1E-8
            super().__init__(log=log)

        def read_status(self, register, wait=0.1):
            if not self.connected:
                return False

            cmd = self.commands.read_status + str(register)
            self.send_command(cmd)
            _time.sleep(wait)
            return self.read_from_device()

        def read_encoder(self, wait=0.1):
            if not self.connected:
                return False

            self.send_command(self.commands.read_counter)
            _time.sleep(wait)
            return self.read_from_device()

        def configure_trig_encoder(
                self, encoder_resolution, direction,
                start_trigger, nr_intervals,
                interval_size, wait=0.1):
            if not self.connected:
                return False

            cmd = (
                self.commands.trigger_source_encoder +
                str(int(int(encoder_resolution)/4)))
            self.send_command(cmd)
            _time.sleep(wait)

            cmd = (
                self.commands.trigger_sequence +
                str(direction) + ',' +
                str(start_trigger) + '/' +
                str(nr_intervals) + ',' +
                str(interval_size)
            )
            self.send_command(cmd)
            _time.sleep(wait)

            return True

        def configure_encoder_reading(self, encoder_resolution):
            if not self.connected:
                return False

            cmd = (
                self.commands.trigger_source_encoder +
                str(int(int(encoder_resolution)/4)))
            self.send_command(cmd)
            return True

        def configure_homing(self, direction, wait=0.1):
            if not self.connected:
                return False

            self.send_command(self.commands.reset_counter)
            _time.sleep(wait)

            cmd = self.commands.search_index + str(direction)
            self.send_command(cmd)
            _time.sleep(wait)
            return True

        def offset_mode_on(self, channel, gain, wait=0.1):
            if not self.connected:
                return False

            self.send_command(self.commands.clear_over_range)
            _time.sleep(wait)

            self.send_command(self.commands.gain + str(gain))
            _time.sleep(wait)

            self.send_command(self.commands.offset_mode + channel + ',1')
            return True

        def offset_mode_off(self, channel):
            if not self.connected:
                return False

            self.send_command(self.commands.offset_mode + channel + ',0')
            return True

    return PDI5150


PDI5150GPIB = PDI5150_factory(_utils.GPIBInterface)
