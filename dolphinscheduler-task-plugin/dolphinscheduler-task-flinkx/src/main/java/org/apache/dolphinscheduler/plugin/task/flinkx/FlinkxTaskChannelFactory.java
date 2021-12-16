package org.apache.dolphinscheduler.plugin.task.flinkx;

import com.google.auto.service.AutoService;
import org.apache.dolphinscheduler.spi.params.base.PluginParams;
import org.apache.dolphinscheduler.spi.task.TaskChannel;
import org.apache.dolphinscheduler.spi.task.TaskChannelFactory;

import java.util.List;

@AutoService(TaskChannelFactory.class)
public class FlinkxTaskChannelFactory implements TaskChannelFactory {

    @Override
    public TaskChannel create() {
        return new FlinkxTaskChannel();
    }

    @Override
    public String getName() {
        return "FLINKX";
    }

    @Override
    public List<PluginParams> getParams() {
        return null;
    }
}
