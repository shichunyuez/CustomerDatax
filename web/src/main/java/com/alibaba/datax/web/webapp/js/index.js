/**
 * Created by Administrator on 2016/10/18 0018.
 */
var sourcePlatformId;
var targetPlatformId;
var sourceAdditionalConf;
var targetAdditionalConf;
function validateDataLinkData()
{
    sourcePlatformId=$('#index-source-platform-select').combobox('getValue');
    if(sourcePlatformId==1)
    {
        sourceAdditionalConf=getSourceBigDataHiveAdditionalConf();
        if(sourceAdditionalConf==null)
        {
            alert('请选择源端.');
            return false;
        }
        else
        {
            if(sourceAdditionalConf.type!=1 && sourceAdditionalConf.type!=2)
            {
                alert("请选择源端为Hive表或为Hive SQL脚本.");
                return false;
            }

            if(sourceAdditionalConf.type==2 && sourceAdditionalConf.sql=='')
            {
                alert("请填写Hive SQL脚本.");
                return false;
            }
        }
    }
    else
    {
        alert('暂不支持此源端平台.');
        return false;
    }

    targetPlatformId=$('#index-target-platform-select').combobox('getValue');
    if(targetPlatformId==2)
    {
        targetAdditionalConf=getTargetBigDataOracleAdditionalConf();
        if(targetAdditionalConf==null)
        {
            alert('请选择目标端.');
            return false;
        }
    }
    else
    {
        alert('暂不支持此目标端平台.');
        return false;
    }

    return true;
}

function addDataLink()
{
    if(!validateDataLinkData())
    {
        return;
    }

    var param={"sourceplatform":sourcePlatformId,"targetplatform":targetPlatformId,"sourceaddconf":sourceAdditionalConf,"targetaddconf":targetAdditionalConf};
    $.ajax({type:"post",contentType: "application/json; charset=utf-8",url:"adddatalink",data:$.toJSON(param),dataType:"json",success: function(resp){
        if(resp!=null)
        {
            if(resp.error!=null)
            {
                $.messager.show({
                    title : '错误',
                    msg : resp.error
                });
            }
            else
            {
                alert('数据中转链路添加成功.');
            }
        }
        else
        {
            alert("错误: 服务器返馈异常. "+resp);
        }

    },error: function (XMLHttpRequest, textStatus, errorThrown) {
        alert("错误: 与服务器通信异常, "+errorThrown+"    textStatus: "+textStatus);
    }});
}

function initSourceAdditionalConfPannel()
{
    var platformId=$('#index-source-platform-select').combobox('getValue');
    if(platformId==1)
    {
        $("#index-source-edit-additionalconf").load("/pages/tags/sourcebigdatahive.html");
    }
    else if(platformId==2)
    {
    }
    else if(platformId==3)
    {
    }
}

function initTargetAdditionalConfPannel()
{
    var platformId=$('#index-target-platform-select').combobox('getValue');
    if(platformId==1)
    {

    }
    else if(platformId==2)
    {
        $("#index-target-edit-additionalconf").load("/pages/tags/targetbigdataoracle.html");
    }
    else if(platformId==3)
    {
    }
}

function initIndexTabs()
{
    $('#index-source-platform-select').combobox({
        width:'180px',
        onSelect:function(record){initSourceAdditionalConfPannel();}
    });
    $('#index-target-platform-select').combobox({
        width:'180px',
        onSelect:function(record){initTargetAdditionalConfPannel();}
    });

    $('#index-source-platform-select').combobox('setValue',1);
    $('#index-target-platform-select').combobox('setValue',2);

    initSourceAdditionalConfPannel();
    initTargetAdditionalConfPannel();

    $('#index-adddatalink-submmit-button').linkbutton({
        iconCls: 'icon-ok',
    }).bind('click', function(){
        addDataLink();
    });
}

$( function() {
    alert('0000');
    initIndexTabs();
    alert('1111');
} );