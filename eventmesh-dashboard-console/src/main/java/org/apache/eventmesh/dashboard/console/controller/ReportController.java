/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eventmesh.dashboard.console.controller;

import org.apache.eventmesh.dashboard.console.function.report.ReportHandlerManage;
import org.apache.eventmesh.dashboard.console.function.report.iotdb.kafka.KafkaMetricStorageException;
import org.apache.eventmesh.dashboard.console.function.report.iotdb.kafka.KafkaReportService;
import org.apache.eventmesh.dashboard.console.function.report.model.MultiGeneralReportDO;
import org.apache.eventmesh.dashboard.console.function.report.model.SingleGeneralReportDO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("report")
public class ReportController {


    @Autowired
    private ReportHandlerManage reportHandlerManage;

    @Autowired
    private KafkaReportService kafkaReportService;

    @RequestMapping("reportByHome")
    public Map<String, List<Map<String, Object>>> reportByHome(@RequestBody MultiGeneralReportDO multiGeneralReportDO) {
        //
        List<SingleGeneralReportDO> singleGeneralReportDOList = new ArrayList<>();
        multiGeneralReportDO.getReportNameList().forEach(reportName -> {
            SingleGeneralReportDO singleGeneralReportDO = new SingleGeneralReportDO();
            BeanUtils.copyProperties(multiGeneralReportDO, singleGeneralReportDO);
            singleGeneralReportDO.setReportName(reportName);
            singleGeneralReportDOList.add(singleGeneralReportDO);
        });
        return queryReports(singleGeneralReportDOList);

    }

    @RequestMapping("reportBySingle")
    public List<Map<String, Object>> reportBySingle(@RequestBody SingleGeneralReportDO singleGeneralReportDO) {

        Map<String, List<Map<String, Object>>> data = queryReports(List.of(singleGeneralReportDO));
        return data.get(singleGeneralReportDO.getReportName());
    }

    private Map<String, List<Map<String, Object>>> queryReports(List<SingleGeneralReportDO> reports) {
        List<SingleGeneralReportDO> kafkaReports = new ArrayList<>();
        List<SingleGeneralReportDO> legacyReports = new ArrayList<>();
        for (SingleGeneralReportDO report : reports) {
            if (kafkaReportService.supports(report)) {
                kafkaReports.add(report);
            } else {
                legacyReports.add(report);
            }
        }
        Map<String, List<Map<String, Object>>> results = new HashMap<>();
        try {
            kafkaReports.forEach(kafkaReportService::validate);
            for (SingleGeneralReportDO report : kafkaReports) {
                results.put(report.getReportName(), kafkaReportService.query(report));
            }
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid Kafka metric query", e);
        } catch (SecurityException e) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Kafka metric scope is not authorized", e);
        } catch (KafkaMetricStorageException | IllegalStateException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka metrics are unavailable", e);
        }
        if (!legacyReports.isEmpty()) {
            results.putAll(reportHandlerManage.queryResultIsMap(legacyReports));
        }
        return results;
    }

    public void reportByMulti() {
    }

}
