package com.alibaba.datax.web;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.request.RequestContextListener;
import org.springframework.web.context.support.XmlWebApplicationContext;

import java.io.File;

/**
 * Created by Administrator on 2017/7/12 0012.
 */
public class Main {

    private static int webPort=8080;

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String home=System.getProperty("datax.home");
        String portStr=System.getProperty("datax.web.port");
        /*if(StringUtils.isBlank(home))
        {
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "需要指定参数datax.home,你需要在运行的时候添加JVM参数-Ddatax.home=***.");
        }
        if(!StringUtils.isBlank(portStr))
        {
            try
            {
                webPort=Integer.parseInt(portStr);
            }
            catch(Exception e)
            {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "请设置正确的datax.web.port参数.");
            }
        }*/
        ApplicationConfig applicationConfig = new ApplicationConfig();
        ServletHolder jerseyServlet = new ServletHolder(new ServletContainer(applicationConfig));

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        context.setResourceBase(home+ File.separator+"webapp");
        context.addServlet(jerseyServlet, "/rest/*");
        context.addServlet(DefaultServlet.class, "/");
        context.addEventListener(new ContextLoaderListener());
        context.addEventListener(new RequestContextListener());

        //context.setInitParameter("contextClass", AnnotationConfigWebApplicationContext.class.getName());
        //context.setInitParameter("contextConfigLocation", SpringJavaConfiguration.class.getName());

        context.setInitParameter("contextClass", XmlWebApplicationContext.class.getName());
        context.setInitParameter("contextConfigLocation", "classpath:applicationContext.xml");

        Server server = new Server(webPort);
        server.setHandler(context);
        try {
            server.start();
            server.join();
        } catch (Exception e) {
            LOG.info("DataX Web启动失, 错误信息为: ",e);
            //throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "DataX Web启动失败.端口为: "+webPort+", webapp根目录为: "+home+ File.separator+"webapp");
        }
    }

}
