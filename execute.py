# -*- coding:utf-8 -*-
import db
import os
import subprocess
import time
import random
import log
import re
import sys
from prettytable import PrettyTable

db_file = f'/backup/checksum.json'

logger = log.Log()
db = db.Database(db_file)

def execute_cmd(cmd, timeout=60):
    p = subprocess.Popen(cmd, stderr=subprocess.STDOUT,
                         stdout=subprocess.PIPE, shell=True)
    t_beginning = time.time()
    seconds_passed = 0
    if timeout > 0:
        while True:
            if p.poll() is not None:
                break
            seconds_passed = time.time() - t_beginning
            if timeout and seconds_passed > timeout:
                p.terminate()
                raise TimeoutError(cmd, timeout)
            time.sleep(0.1)
    output = p.stdout.read().decode()
    return output

def findsdevice(snapshot):
    finddevice = f"/sbin/lvdisplay | grep {snapshot} | awk '''{{print $3}}''' | awk '''{{print $1}}''' | head -n 1"
    sdevice = execute_cmd(finddevice)
    return sdevice.strip()

def getMD5(file_name):
    if checkfiledir(file_name):
        return execute_cmd("md5sum {}".format(file_name), 0)
    else:
        return False

def checkresource(resource):
    res = execute_cmd(f"linstor r l | grep {resource}", 0)
    if res:
        return True
    else:
        return False

def checkfiledir(checkarg):
    return os.path.exists(checkarg)

def checkimage(resource):
    print(f"检查资源：{resource}的映像文件数量")
    data = db.finddb("resource", resource)
    count = 0
    flag = 0
    if data:
        i = len(data)
    else:
        i = 0
    while i > 0:
        if data[i-1]["path"]:
            count = count + 1
            flag = i-1
        i = i - 1
    if count >= 3:
        delpath = data[flag]["path"]
        print(f"资源：{resource}的映像文件数量超过3个, 最旧的映像文件：{delpath}将会被删除")
        logger.write_to_log("INFO", f"资源：{resource}的映像文件数量超过3个, 最旧的映像文件：{delpath}将会被删除", True)
        result = execute_cmd("rm " + data[flag]["path"])
        if result:
            logger.write_to_log("ERR", f"删除映像文件：{delpath}失败", True)
            print(f"删除映像文件：{delpath}失败")
            return False
        res = db.changedb(data[flag]["snapshot"])
        if res:
            logger.write_to_log("ERR", f"在数据库删除映像文件：{delpath}失败", True)
            print(f"在数据库删除映像文件：{delpath}失败")
            return False
        logger.write_to_log("INFO", f"成功在数据库删除映像文件：{delpath}", True)
        print(f"成功在数据库删除映像文件：{delpath}")
        return True
    else:
        return True

def checkSnapshot(resource):
    print(f"检查资源：{resource}的快照数量")
    data = db.finddb("resource", resource)
    if data:
        lena = len(data)
    else:
        lena = 0
    if lena >= 5:
        deldata = data[0]
        delsp = data[0]["snapshot"]
        print(f"资源：{resource}的快照数量超过5个, 最旧的快照： {delsp}将会被删除")
        logger.write_to_log("INFO", f"资源：{resource}的快照数量超过5个, 最旧的快照： {delsp}将会被删除", True)
        result = execute_cmd("linstor snapshot delete " + resource + " " + data[0]["snapshot"])
        if "ERROR" in result:
            logger.write_to_log("ERR", f"删除快照：{delsp}失败", True)
            print(f"删除快照：{delsp}失败")
        if data[0]["path"]:
            delpath = data[0]["path"]
            result = execute_cmd("rm " + data[0]["path"])
            if result:
                logger.write_to_log("ERR", f"删除映像文件：{delpath}失败", True)
                print(f"删除映像文件：{delpath}失败")
        res = db.deletedb(data[0]["snapshot"])
        if not res:
            logger.write_to_log("ERR", f"在数据库删除快照：{delsp}失败", True)
            print(f"在数据库删除快照：{delsp}失败")
            return False
        logger.write_to_log("INFO", f"成功在数据库删除快照：{delsp}", True)
        print(f"成功在数据库删除快照：{delsp}")
        return True
    else:
        return True

def create_snap(res_name):
    if not checkresource(res_name):
        logger.write_to_log("ERR", f"资源：{res_name}不存在", True)
        print(f"资源：{res_name}不存在")
        return 
    if not checkSnapshot(res_name):
        logger.write_to_log("ERR", f"为资源：{res_name}创建快照失败", True)
        print(f"为资源：{res_name}创建快照失败")
        return
    stime = time.strftime('%Y-%m-%d %H:%M:%S ',time.localtime(time.time()))
    ntime = re.sub(u"([^\u0030-\u0039])", "", stime)
    snap_name = res_name + str(ntime)
    logger.write_to_log("INFO", f"为资源：{res_name}新建的快照名为：{snap_name}", True)
    print(f"开始为资源：{res_name}创建快照：{snap_name}")
    result = execute_cmd("linstor snapshot create " + res_name + " " + snap_name)
    if "SUCCESS" in result:
        logger.write_to_log("INFO", f"成功为资源：{res_name}创建快照，快照名为：{snap_name}", True)
        print(f"成功创建快照：{snap_name}")
        sd = findsdevice(snap_name)
        if sd:
            logger.write_to_log("INFO", f"快照对应的设备盘为：{sd}", True)
        else:
            print("没找到快照的设备盘，可能设备盘不在此节点")
            logger.write_to_log("ERR", f"没找到快照的设备盘，可能设备盘不在此节点", True)
            return
        print(f"开始计算快照：{snap_name}的校验和")
        sd_md5 = getMD5(sd).split(" ")[0]
        logger.write_to_log("INFO", f"获取快照设备的校验和，并记录在数据库中", True)
        db.insertdb(res_name, snap_name, sd_md5, stime, "")
        print(f"成功为资源：{res_name}创建快照")
    else:
        logger.write_to_log("ERR", f"为资源：{res_name}创建快照失败", True)
        print(f"资源：{res_name}备份失败")

def checkMD5(resource, snapshot):
    print("对比快照设备的校验值和数据库的校验值是否一致")
    logger.write_to_log("INFO", f"对比快照设备的校验值和数据库的校验值是否一致", True)
    backup_record = db.selectdb("resource", resource, "snapshot", snapshot)
    if not backup_record:
        logger.write_to_log("ERR", f"找不到资源：{resource}或快照：{snapshot}", True)
        print(f"找不到资源：{resource}或快照：{snapshot}")
        return
    device = findsdevice(backup_record["snapshot"])
    md5_dev = getMD5(device).split(" ")[0]
    if md5_dev == backup_record["snap_md5"]:
        logger.write_to_log("INFO", f"快照设备的数据未被损坏，可用于还原", True)
        print("快照设备的数据未被损坏，可用于还原")
        return backup_record
    else:
        return False

def restore_file(resource, snapshot):
    res = restore_block(resource, snapshot)
    if res:
        execute_cmd("mkdir " + "/mnt/" + snapshot + "_sn")
        print("还原后的设备：")
        cmd = f"linstor r lv | grep {res} | head -n 1 | awk '''{{print $12}}'''"
        sd = execute_cmd(cmd, 0).strip()
        print(sd)
        #sd = findsdevice(backup_record["snapshot"])
        results = execute_cmd("mount " + sd + " /mnt/" + snapshot + "_sn")
        if "wrong" in results:
            logger.write_to_log("ERR", f"还原后的设备：{sd}挂载到目录：/mnt/{snapshot}_sn失败", True)
        else:
            logger.write_to_log("INFO", f"还原后的设备：{sd}挂载成功, 挂载目录为：/mnt/{snapshot}_sn", True)
            print("挂载目录为：" + "/mnt/" + snapshot + "_sn")

def restore_block(resource, snapshot):
    res = checkMD5(resource, snapshot)
    if res:
        print(f"使用快照：{snapshot}对资源：{resource}进行还原")
        r1 = execute_cmd("linstor rd c " + snapshot + "_b")
        if "ERROR" in r1:
            print(f"创建资源定义：{snapshot}_b 失败")
            logger.write_to_log("ERR", f"创建资源定义：{snapshot}_b 失败", True)
            return
        logger.write_to_log("INFO", f"成功创建资源定义：{snapshot}_b", True)
        r2 = execute_cmd("linstor snapshot volume-definition restore --from-resource " + resource +
               " --from-snapshot " + snapshot + " --to-resource " + snapshot + "_b")
        if "ERROR" in r2:
            print(f"创建卷定义：{snapshot}_b 失败")
            logger.write_to_log("ERR", f"创建卷定义：{snapshot}_b 失败", True)
            return
        logger.write_to_log("INFO", f"成功创建卷定义：{snapshot}_b", True)
        r3 = execute_cmd("linstor snapshot resource restore --from-resource " + resource +
                    " --from-snapshot " + snapshot + " --to-resource " + snapshot + "_b")
        if "SUCCESS" in r3:
            res_name = snapshot + "_b"
            logger.write_to_log("INFO", f"成功还原资源，资源名为：{snapshot}_b", True)
            print("成功还原资源，资源名为：" + snapshot + "_b")
            return res_name
        else:
            logger.write_to_log("ERR", f"还原资源：{snapshot}_b 失败", True)
            return
    else:
        print(f"校验和不匹配, 快照设备：{snapshot}可能被损坏")
        logger.write_to_log("ERR", f"校验和不匹配, 快照设备：{snapshot}可能被损坏", True)
        return

def show_snap(res_name, snapshot_name):
    if res_name:
        print(f"显示资源：'{res_name}'的备份信息：")
        logger.write_to_log("INFO", f"显示资源：'{res_name}'的备份信息", True)
    elif snapshot_name:
        print(f"显示快照：'{snapshot_name}'的备份信息：")
        logger.write_to_log("INFO", f"显示快照：'{snapshot_name}'的备份信息", True)
    else:
        print("显示所有资源的备份信息：")
        logger.write_to_log("INFO", f"显示所有资源的备份信息", True)

    data = db.printdb()
    header = ["资源名", "快照", "校验和", "时间", "映像文件"]
    table = PrettyTable()
    table.field_names = header
    for item in data:
        if res_name:
            if item["resource"] == res_name:
                table.add_row(item.values())
        elif snapshot_name:
            if item["snapshot"] == snapshot_name:
                table.add_row(item.values())
        else:
            table.add_row(item.values())
    print(table)
  
def dump_snap(res_name, backup_dir):
    if not checkresource(res_name):
        logger.write_to_log("ERR", f"资源：{res_name}不存在", True)
        print(f"资源：{res_name}不存在")
        return 

    if not checkfiledir(backup_dir):
        logger.write_to_log("ERR", f"路径：{backup_dir}不存在", True)
        print(f"路径：{backup_dir}不存在")
        return

    if not checkimage(res_name):
        return

    if not os.path.getsize(db_file):
        logger.write_to_log("ERR", f"数据库：{db_file}不存在", True)
        print(f"数据库：{db_file}不存在")
        os.remove(db_file)
        return

    backup_record = db.searchdb("resource",res_name)
    snap_name = backup_record["snapshot"]
    snap_md5 = backup_record["snap_md5"]
    snap_device = findsdevice(snap_name)
    backup_dir = os.path.abspath(backup_dir)
    if not snap_device:
        logger.write_to_log("ERR", f"无法找到设备：{snap_device}", True)
        return
    img_file_name = "{}/{}.img".format(backup_dir,snap_name)
    print(f"开始将资源：{res_name}的快照：{snap_name}导出成映像文件：{img_file_name}")
    logger.write_to_log("INFO", f"开始将快照：{snap_name}导出成映像文件：{img_file_name}", True)
    result = execute_cmd("dd if={} of={} bs=1M oflag=direct status=progress".format(snap_device,img_file_name), 0)
    #print(result)
    if "copied" in result:
        logger.write_to_log("INFO", f"成功将快照：{snap_name}导出成映像文件：{img_file_name}", True)
        print(f"成功将快照：{snap_name}导出成映像文件：{img_file_name}")
        logger.write_to_log("INFO", f"开始计算映像文件：{img_file_name}的校验和", True)
        file_md5 = getMD5(img_file_name).split(" ")[0]
        if snap_md5 == file_md5:
            logger.write_to_log("INFO", f"映像文件{img_file_name}的校验和和快照的一致，所以保存映像文件", True)
            db.updatedb(snap_name, img_file_name)
            fcap = execute_cmd(f"df -h '{backup_dir}' | sed -n 2p | awk '''{{print $4}}'''", 0).replace('\n', '').replace('\r', '')
            fuse = execute_cmd(f"df -h '{backup_dir}' | sed -n 2p | awk '''{{print $5}}'''", 0).replace('\n', '').replace('\r', '')
            print(f"{backup_dir}剩余存储空间为：{fcap}")
            logger.write_to_log("INFO", f"{backup_dir}剩余存储空间为：{fcap}", True)
            print(f"成功将资源：{res_name}的快照导出成映像文件：{img_file_name}")
            if fuse > "80%":
                print("备份剩余空间不足20%")
                logger.write_to_log("ALERT", f"备份剩余空间不足20%", True)
        else:
            print(f"映像文件：{img_file_name}的校验和和快照的不一致，导出失败")
            logger.write_to_log("ERR", f"映像文件：{img_file_name}的校验和和快照的不一致，导出失败", True)
    else:
        logger.write_to_log("ERR", f"将快照：{snap_name}导出成映像文件：{img_file_name}失败", True)
        print(f"将快照：{snap_name}导出成映像文件：{img_file_name}失败")

def image_restore_file(resource, path, vg):
    cmd = f"vgs | grep {vg}"
    if not execute_cmd(cmd, 0):
        logger.write_to_log("ERR", f"卷组：{vg}不存在", True)
        print(f"卷组：{vg}不存在")
        return
    print("比较映像文件的校验和和数据库记录的校验和")
    logger.write_to_log("INFO", f"比较映像文件的校验和和数据库记录的校验和", True)
    backup_record = db.selectdb("resource", resource, "path", path)
    if not backup_record:
        logger.write_to_log("ERR", f"资源：{resource}或者映像文件：{path}不存在", True)
        print(f"资源：{resource}或者映像文件：{path}不存在")
        return
    image_record = getMD5(path).split(" ")[0]
    backup_sp = backup_record["snapshot"]
    if backup_record["snap_md5"] == image_record:
        logger.write_to_log("INFO", f"映像文件的校验和和数据库记录的校验和一致", True)
        print(f"映像文件的校验和和数据库记录的校验和一致")
        logger.write_to_log("INFO", f"开始对资源：{resource}进行恢复", True)
        print(f"开始对资源：{resource}进行恢复")
        res_name = f"{backup_sp}_r"
        cmd = f"ls -lh {path} | awk '''{{print $5}}'''"
        lv_size = execute_cmd(cmd).strip()
        cmd = f"lvcreate -Zn -n {backup_sp}_r -L {lv_size} {vg}"
        result = execute_cmd(cmd)
        if "created" in result:
            cmd = f"/sbin/lvdisplay | grep {backup_sp}_r | awk '''{{print $3}}''' | awk '''{{print $1}}''' | head -n 1"
            device = execute_cmd(cmd).strip()
            if not device:
                print(f"设备：{device}创建失败")
                logger.write_to_log("ERR", f"设备：{device}创建失败，资源恢复失败", True)
            print(f"设备：{device}创建成功")
            logger.write_to_log("ERR", f"设备：{device}创建成功", True)
            time.sleep(5)
            results = execute_cmd(f"mkfs.ext4 {device}", 0)
            if "done" not in results:
                logger.write_to_log("ERR", f"格式化设备：{device}失败", True)
                print(f"格式化设备：{device}失败")
                logger.write_to_log("ERR", f"资源：{resource}恢复失败", True)
                print(f"资源：{resource}恢复失败")
                return

            cmd = f"dd if={path} of={device} bs=1M status=progress"
            result = execute_cmd(cmd, 0)
            execute_cmd("mkdir " + "/mnt/" + backup_record["snapshot"])
            results = execute_cmd("mount " + device + " /mnt/" + backup_record["snapshot"])
            if "wrong" in results:
                print(f"挂载{device}失败")
                logger.write_to_log("ERR", f"挂载{device}失败", True)
            else:
                logger.write_to_log("INFO", f"成功挂载{device}，挂载目录为：/mnt/{backup_record['''snapshot''']}", True)
                print(f"成功挂载{device}，挂载目录为：/mnt/{backup_record['''snapshot''']}")
        else:
            print(f"资源：{resource}恢复失败")
            logger.write_to_log("ERR", f"资源：{resource}恢复失败", True)
    else:
        print(f"校验和不匹配, 映像文件：{path}可能被损坏")
        logger.write_to_log("ERR", f"校验和不匹配, 映像文件：{path}可能被损坏", True)

def image_restore_block(resource, path, vg):
    cmd = f"vgs | grep {vg}"
    if not execute_cmd(cmd, 0):
        logger.write_to_log("ERR", f"卷组：{vg}不存在", True)
        print(f"卷组：{vg}不存在")
        return
    print("比较映像文件的校验和和数据库记录的校验和")
    logger.write_to_log("INFO", f"比较映像文件的校验和和数据库记录的校验和", True)
    backup_record = db.selectdb("resource", resource, "path", path)
    if not backup_record:
        logger.write_to_log("ERR", f"资源：{resource}或者映像文件：{path}不存在", True)
        print(f"资源：{resource}或者映像文件：{path}不存在")
        return
    image_record = getMD5(path).split(" ")[0]
    backup_sp = backup_record["snapshot"]
    if backup_record["snap_md5"] == image_record:
        logger.write_to_log("INFO", f"映像文件的校验和和数据库记录的校验和一致", True)
        print(f"映像文件的校验和和数据库记录的校验和一致")
        logger.write_to_log("INFO", f"开始对资源：{resource}进行恢复", True)
        print(f"开始对资源：{resource}进行恢复")
        cmd = f"ls -lh {path} | awk '''{{print $5}}'''"
        lv_size = execute_cmd(cmd).strip()
        cmd = f"lvcreate -Zn -n {backup_sp}_r -L {lv_size} {vg}"
        result = execute_cmd(cmd)
        if "created" in result:
            cmd = f"/sbin/lvdisplay | grep {backup_sp}_r | awk '''{{print $3}}''' | awk '''{{print $1}}''' | head -n 1"
            device = execute_cmd(cmd)
            if not device:
                print(f"设备：{device}创建失败")
                logger.write_to_log("ERR", f"设备：{device}创建失败，资源恢复失败", True)
            print(f"设备：{device}创建成功")
            logger.write_to_log("ERR", f"设备：{device}创建成功", True)
            cmd = f"dd if={path} of={device} bs=1M status=progress"
            result = execute_cmd(cmd, 0)
            res_name = f"{backup_sp}_r"
            logger.write_to_log("INFO", f"资源恢复成功，恢复后的资源名为：{backup_sp}_r", True)
            print("资源恢复成功，恢复后的资源名为：" + backup_sp + "_r")
        else:
            print(f"资源：{resource}恢复失败")
            logger.write_to_log("ERR", f"资源：{resource}恢复失败", True)
    else:
        print(f"校验和不匹配, 映像文件：{path}可能被损坏")
        logger.write_to_log("ERR", f"校验和不匹配, 映像文件：{path}可能被损坏", True)

