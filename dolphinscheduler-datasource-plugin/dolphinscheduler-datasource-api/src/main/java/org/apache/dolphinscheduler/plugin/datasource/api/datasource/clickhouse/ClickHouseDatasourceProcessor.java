/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.datasource.api.datasource.clickhouse;

import org.apache.dolphinscheduler.plugin.datasource.api.datasource.AbstractDatasourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.utils.Constants;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ClickHouseDatasourceProcessor extends AbstractDatasourceProcessor {

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        ClickhouseConnectionParam connectionParams = (ClickhouseConnectionParam) createConnectionParams(connectionJson);

        ClickHouseDatasourceParamDTO clickHouseDatasourceParamDTO = new ClickHouseDatasourceParamDTO();
        clickHouseDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        clickHouseDatasourceParamDTO.setUserName(connectionParams.getUser());
        clickHouseDatasourceParamDTO.setOther(parseOther(getDbType(), connectionParams.getOther()));

        String[] hostSeperator = connectionParams.getAddress().split(Constants.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constants.COMMA);
        clickHouseDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constants.COLON)[1]));
        clickHouseDatasourceParamDTO.setHost(hostPortArray[0].split(Constants.COLON)[0]);

        return clickHouseDatasourceParamDTO;
    }

    @Override
    public ConnectionParam createConnectionParams(BaseDataSourceParamDTO datasourceParam) {
        ClickHouseDatasourceParamDTO clickHouseParam = (ClickHouseDatasourceParamDTO) datasourceParam;
        String address = String.format("%s%s:%s", Constants.JDBC_CLICKHOUSE, clickHouseParam.getHost(), clickHouseParam.getPort());
        String jdbcUrl = address + "/" + clickHouseParam.getDatabase();

        ClickhouseConnectionParam clickhouseConnectionParam = new ClickhouseConnectionParam();
        clickhouseConnectionParam.setDatabase(clickHouseParam.getDatabase());
        clickhouseConnectionParam.setAddress(address);
        clickhouseConnectionParam.setJdbcUrl(jdbcUrl);
        clickhouseConnectionParam.setUser(clickHouseParam.getUserName());
        clickhouseConnectionParam.setPassword(PasswordUtils.encodePassword(clickHouseParam.getPassword()));
        clickhouseConnectionParam.setDriverClassName(getDatasourceDriver());
        clickhouseConnectionParam.setValidationQuery(getValidationQuery());
        clickhouseConnectionParam.setOther(transformOther(getDbType(), clickHouseParam.getOther()));
        clickhouseConnectionParam.setProps(clickHouseParam.getOther());
        return clickhouseConnectionParam;
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, ClickhouseConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return Constants.COM_CLICKHOUSE_JDBC_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return Constants.CLICKHOUSE_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        ClickhouseConnectionParam clickhouseConnectionParam = (ClickhouseConnectionParam) connectionParam;
        String jdbcUrl = clickhouseConnectionParam.getJdbcUrl();
        if (!StringUtils.isEmpty(clickhouseConnectionParam.getOther())) {
            jdbcUrl = String.format("%s?%s", jdbcUrl, clickhouseConnectionParam.getOther());
        }
        return jdbcUrl;
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        ClickhouseConnectionParam clickhouseConnectionParam = (ClickhouseConnectionParam) connectionParam;
        Class.forName(getDatasourceDriver());
        return DriverManager.getConnection(getJdbcUrl(clickhouseConnectionParam),
                clickhouseConnectionParam.getUser(), PasswordUtils.decodePassword(clickhouseConnectionParam.getPassword()));
    }

    @Override
    public DbType getDbType() {
        return DbType.CLICKHOUSE;
    }

}
