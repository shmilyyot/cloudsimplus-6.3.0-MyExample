/*
 * CloudSim Plus: A modern, highly-extensible and easier-to-use Framework for
 * Modeling and Simulation of Cloud Computing Infrastructures and Services.
 * http://cloudsimplus.org
 *
 *     Copyright (C) 2015-2018 Universidade da Beira Interior (UBI, Portugal) and
 *     the Instituto Federal de Educação Ciência e Tecnologia do Tocantins (IFTO, Brazil).
 *
 *     This file is part of CloudSim Plus.
 *
 *     CloudSim Plus is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     CloudSim Plus is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with CloudSim Plus. If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * Implements a set of testbeds in a repeatable manner, allowing a researcher to execute several simulation runs
 * for a given experiment and collect statistical data using a scientific approach.
 * It represents real testbeds implemented to assess CloudSim Plus features, providing relevant results.
 *
 * <p>Each package contains the classes for a specific testbed that is composed of:</p>
 * <ul>
 *     <li>a {@link org.cloudsimplus.testbeds.Experiment} that implements a single run
 *     of a specific simulation scenario. It usually has a main method just to
 *     check the execution of the experiment in an isolated way.</li>
 *     <li>a {@link org.cloudsimplus.testbeds.ExperimentRunner} that is accountable for
 *     running a specific SimulationExperiment different times with;
 *     different configurations (such as seeds, number of VMs, Cloudlets, etc),
 *     showing scientific results at the end of the execution.</li>
 * </ul>
 *
 * @author Manoel Campos da Silva Filho
 */
package org.cloudsimplus.testbeds;
