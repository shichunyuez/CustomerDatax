package com.alibaba.datax.server.container;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.datax.server.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.server.communicator.TaskContainerCommunicator;
import com.alibaba.datax.server.communicator.TaskGroupContainerCommunicator;
import com.alibaba.datax.server.communicator.communication.Communication;
import com.alibaba.datax.server.util.CoreConstant;
import com.alibaba.datax.server.util.FrameworkErrorCode;
import com.alibaba.datax.server.work.AbstractWork;
import com.alibaba.datax.server.work.Task;
import com.alibaba.datax.server.work.TaskGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Administrator on 2017/7/26 0026.
 */
public class TaskContainer extends AbstractContainer {

    private static final Logger LOG = LoggerFactory
            .getLogger(TaskContainer.class);

    private Map<Integer, Task> taskMap=null;
    private List<Task> taskQueueWaiting=null;
    private Map<Integer, Task> taskFailedMap=null;
    private List<Task> runingTasks=null;
    private Map<Integer, Long> taskStartTimeMap=null;

    private Map<Integer, Task> buildTaskMap(List<AbstractWork> tasks){
        Map<Integer, Task> map = new HashMap<Integer, Task>();
        for(AbstractWork task : tasks){
            int taskId = task.getWorkID();
            map.put(taskId, (Task)task);
        }
        return map;
    }

    private List<Task> buildRemainTasks(List<AbstractWork> tasks){
        List<Task> remainTasks = new LinkedList<Task>();
        for(AbstractWork task : tasks){
            remainTasks.add((Task)task);
        }
        return remainTasks;
    }

    @Override
    public AbstractContainerCommunicator initContainerCommunicator() {
        return new TaskContainerCommunicator(this);
    }

    public TaskContainer(TaskGroup taskGroup, List<AbstractWork> subWorks) {
        super(taskGroup,subWorks);

        this.taskMap = buildTaskMap(subWorks); //taskId与task配置
        this.taskQueueWaiting = buildRemainTasks(subWorks); //待运行task列表
        this.taskFailedMap = new HashMap<Integer, Task>(); //taskId与上次失败实例
        this.runingTasks = new ArrayList<Task>(subWorks.size()); //正在运行task
        this.taskStartTimeMap = new HashMap<Integer, Long>(); //任务开始时间
    }

    @Override
    public void addCheck() throws Exception{

    }

    @Override
    public void addInit() throws Exception{

    }

    @Override
    public void addPrepare() throws Exception{

    }

    private Task removeTask(List<Task> taskList, int taskId){
        Iterator<Task> iterator = taskList.iterator();
        while(iterator.hasNext()){
            Task task = iterator.next();
            if(task.getWorkID() == taskId){
                iterator.remove();
                return task;
            }
        }
        return null;
    }

    @Override
    public void addSchedule()
    {

    }

    @Override
    public void addPost() throws Exception{

    }

    @Override
    public void addDestroy() throws Exception{

    }

    @Override
    public void logStat() {

    }
}
