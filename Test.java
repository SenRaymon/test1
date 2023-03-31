package com.bdkj.technology.raysen.controller;

import cn.hutool.core.collection.*;
import cn.hutool.http.*;
import com.alibaba.fastjson.*;
import com.baomidou.mybatisplus.core.toolkit.*;
import com.google.common.collect.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class Test {
	/**
	 * 支付流水解析
	 * @author Raysen
	 * @create 2023/1/9 14:58
	 */
	@XxlJob("checkPayTask")
	public ReturnT checkPayTask(){
		BankCashflowAnalysisDetail errorDetail = new BankCashflowAnalysisDetail();
		List<BankCashflowAnalysisExcel> errorExcelList = new ArrayList<>();
		BankCashflowAnalysisExcel currentExcel = new BankCashflowAnalysisExcel();
		try {
			log.info("=====流水解析服务：开始检查任务=====");

			List<BankCashflowAnalysisDetail> taskList = analysisDetailService.getTaskList();
			if (CollectionUtil.isEmpty(taskList)){
				return ReturnT.SUCCESS;
			}

			//使用 Runtime.getRuntime().availableProcessors() 方法获取可用的处理器数量，并将其用作线程池的大小和子列表的大小。
			int availableProcessors = Runtime.getRuntime().availableProcessors();//可用处理器数量
			int partitionSize = (int) Math.ceil(taskList.size() / (double) availableProcessors);//每个处理器处理的任务数量
			// 创建线程池
			ExecutorService executor = Executors.newFixedThreadPool(availableProcessors);

			// 将taskList拆分为多个子列表，每个子列表包含一定数量的元素
			List<List<BankCashflowAnalysisDetail>> partitions = Lists.partition(taskList, partitionSize);
			// 创建Callable任务，每个任务处理一个子列表
			List<Callable<Void>> tasks = partitions.stream()
					.map(partition -> (Callable<Void>) () -> {
						for (BankCashflowAnalysisDetail bankCashflowAnalysisDetail : partition) {
							//取出一个进行解析
							//BankCashflowAnalysisDetail bankCashflowAnalysisDetail = taskList.get(0);

							//当前任务流水号
							String orderNo = bankCashflowAnalysisDetail.getOrderNo();

							errorDetail = bankCashflowAnalysisDetail;
							//==关联EXCL表数据==
							//根据流水号查询待解析的流水批次，取一个进行解析
							LambdaQueryWrapper<BankCashflowAnalysisExcel> excelSuperWrapper = Wrappers.lambdaQuery();
							excelSuperWrapper
									.eq(BankCashflowAnalysisExcel::getOrderNo, bankCashflowAnalysisDetail.getOrderNo())
									.eq(BankCashflowAnalysisExcel::getStatus, 0)
									.eq(BankCashflowAnalysisExcel::getDataStatus, 0);
							//.last("limit 1");

							//如果这里查不到怎么办？能走到这里说明流水的状态是1，但是流水批次却没数据，这种情况会导致一直循环这条数据；如果为空，应该将原流水改为3（待定），先用return处理
							List<BankCashflowAnalysisExcel> excelList = excelService.list(excelSuperWrapper);

							if (CollectionUtil.isEmpty(excelList)) {
								log.info("流水解析服务：detail与excel都有0状态的数据，但此时根据0去查excel却又查不到");
								//更新异常状态
								BankCashflowAnalysisExcel analysisExcel = new BankCashflowAnalysisExcel();
								analysisExcel.setStatus(4);
								//操作失败：未找到流水批次
								analysisExcel.setRemark("文件丢失，请重新提交上传");
								LambdaQueryWrapper<BankCashflowAnalysisExcel> excelWrapper = Wrappers.lambdaQuery();
								excelWrapper
										.eq(BankCashflowAnalysisExcel::getOrderNo, bankCashflowAnalysisDetail.getOrderNo())
										.ne(BankCashflowAnalysisExcel::getStatus, 1);
								excelService.update(analysisExcel, excelWrapper);

								bankCashflowAnalysisDetail.setStatus(4);
								bankCashflowAnalysisDetail.setRemark("文件丢失，请重新提交上传");
								analysisDetailService.updateById(bankCashflowAnalysisDetail);
								//更新状态到risk-value reportHistory
								analysisDetailService.reportChangeCallBack(bankCashflowAnalysisDetail, null);

								analysisExcel.setOrderNo(orderNo);
								analysisExcel.setFileUrl("空");
								errorExcelList.add(analysisExcel);
								continue;
							}
							String errorMsg = null;
							String detailErrorMsg = "";

							// 创建内部线程池
							ExecutorService innerExecutor = Executors.newFixedThreadPool(availableProcessors);
							int innerPartitionSize = (int) Math.ceil(excelList.size() / (double) availableProcessors);//每个处理器处理的任务数量

							// 将taskList拆分为多个子列表，每个子列表包含一定数量的元素
							List<List<BankCashflowAnalysisExcel>> innerPartitions = Lists.partition(excelList, innerPartitionSize);

							// 提交内部任务
							// 创建Callable任务，每个任务处理一个子列表
							List<Callable<Void>> innerTasks = innerPartitions.stream()
									.map(innerPartition -> (Callable<Void>) () -> {
										for (BankCashflowAnalysisExcel analysisExcelOne : innerPartition) {
											currentExcel = analysisExcelOne;

											//当前任务流水批次号
											String excelNo = analysisExcelOne.getExcelNo();
											String targetFileUrl = analysisExcelOne.getFileUrl();

											String suffix = targetFileUrl.substring(targetFileUrl.lastIndexOf('.'));
											//支付流水，需要自动进行pdf转excel
											if (".pdf".equalsIgnoreCase(suffix)) {
												String excelFileUrl = FileUtils.sendBinaryRequest(targetFileUrl);

												BankCashflowAnalysisExcel analysisExcel = new BankCashflowAnalysisExcel();
												analysisExcel.setExcelFileUrl(excelFileUrl);
												LambdaQueryWrapper<BankCashflowAnalysisExcel> excelQueryWrapper = Wrappers.lambdaQuery();
												excelQueryWrapper.eq(BankCashflowAnalysisExcel::getExcelNo, excelNo);
												excelService.update(analysisExcel, excelQueryWrapper);

												targetFileUrl = excelFileUrl;
											}

											//根据文件名判断流水类型
											String flowType = "其他";
											String fileName = analysisExcelOne.getFileName();
											if (StringUtils.isNotEmpty(fileName)) {
												if (fileName.contains("支付宝")) {
													flowType = "支付宝";
												} else if (fileName.contains("微信")) {
													flowType = "微信";
												}
											}

											//请求流水解析，解析文件
											Map<String, Object> paramMap = new HashMap<>();

											paramMap.put("appId", excelNo);
											//请求体
											List<Map<String, Object>> pbocUrlList = new ArrayList<>();
											Map<String, Object> pbocMap = new HashMap<>();
											pbocMap.put("pbocUrl", targetFileUrl);
											pbocMap.put("cname", bankCashflowAnalysisDetail.getPersonName());
											pbocMap.put("ckey", bankCashflowAnalysisDetail.getIdNumber());
											pbocMap.put("fileName", analysisExcelOne.getFileName());
											pbocUrlList.add(pbocMap);
											paramMap.put("data", pbocUrlList);


											log.info("======流水解析，请求参数:{}", JSONObject.toJSONString(paramMap));
											String resStr = HttpUtil.post(cashfloowStatistics + "/excel/pboc", JSONObject.toJSONString(paramMap), 1200000);

											JSONObject resJO = JSONObject.parseObject(resStr);
											log.info("======流水解析，响应:{}", resJO);
											if (resJO.getIntValue("code") != 200) {
												String remark = "出现未知错误文件解析失败";//"流水解析失败：未知错误，请联系管理员";

												if (StringUtils.isNotEmpty(resJO.getString("message")) && !resJO.getString("message").contains("506")) {
													remark = resJO.getString("message").length() >= 255 ? resJO.getString("message").substring(0, 254) : resJO.getString("message");
												} else {
													remark = resJO.getString("message");
												}

												log.info(orderNo + remark);
												errorMsg = remark;
												detailErrorMsg = detailErrorMsg + errorMsg;
												// 操作EXCL表数据
												BankCashflowAnalysisExcel analysisExcel = new BankCashflowAnalysisExcel();
												analysisExcel.setStatus(4);
												analysisExcel.setRemark(remark);
												LambdaQueryWrapper<BankCashflowAnalysisExcel> excelQueryWrapper = Wrappers.lambdaQuery();
												excelQueryWrapper.eq(BankCashflowAnalysisExcel::getExcelNo, excelNo);
												excelService.update(analysisExcel, excelQueryWrapper);
												//throw new ServiceException("操作失败：银行流水解析出错 "+resJO.getString("message"));
												analysisExcelOne.setRemark(remark);
												errorExcelList.add(analysisExcelOne);
												continue;
											}
											//响应数据
											JSONObject dataJO = null;
											try {
												dataJO = resJO.getJSONObject("data");
											} catch (Exception e) {
												log.info(orderNo + "操作成功,但文件未解析到任何数据");
												//操作成功,但文件未解析到任何数据
												errorMsg = "数据解析错误，未解析到任何数据";
												detailErrorMsg = detailErrorMsg + errorMsg;
												// 操作EXCL表数据
												BankCashflowAnalysisExcel analysisExcel = new BankCashflowAnalysisExcel();
												analysisExcel.setStatus(4);
												analysisExcel.setRemark("数据解析错误，未解析到任何数据");
												LambdaQueryWrapper<BankCashflowAnalysisExcel> excelWrapper = Wrappers.lambdaQuery();
												excelWrapper.eq(BankCashflowAnalysisExcel::getExcelNo, excelNo);
												excelService.update(analysisExcel, excelWrapper);

												analysisExcelOne.setRemark(analysisExcel.getRemark());
												errorExcelList.add(analysisExcelOne);
												continue;
											}
											if (dataJO == null) {
												log.info(orderNo + "文档解析结果数据为空，请检查此文件");
												//文档解析结果数据为空，请检查此文件
												errorMsg = "解析结果数据为空，请检查文件内容";
												detailErrorMsg = detailErrorMsg + errorMsg;
												// 操作EXCL表数据
												BankCashflowAnalysisExcel analysisExcel = new BankCashflowAnalysisExcel();
												analysisExcel.setStatus(4);
												analysisExcel.setRemark("解析结果数据为空，请检查文件内容");
												LambdaQueryWrapper<BankCashflowAnalysisExcel> excelWrapper = Wrappers.lambdaQuery();
												excelWrapper.eq(BankCashflowAnalysisExcel::getExcelNo, excelNo);
												excelService.update(analysisExcel, excelWrapper);

												analysisExcelOne.setRemark(errorMsg);
												errorExcelList.add(analysisExcelOne);
												continue;
											}

											JSONArray excelCashFlowJA = dataJO.getJSONArray("excelCashFlow");
											if (excelCashFlowJA == null || excelCashFlowJA.size() == 0) {
												log.info(orderNo + "文档解析结果数据为空，请检查此文件.");
												errorMsg = "解析结果数据为空，请检查文件内容";
												detailErrorMsg = detailErrorMsg + errorMsg;
												// 操作EXCL表数据
												BankCashflowAnalysisExcel analysisExcel = new BankCashflowAnalysisExcel();
												analysisExcel.setStatus(4);
												analysisExcel.setRemark("数据解析错误，未解析到任何数据");
												LambdaQueryWrapper<BankCashflowAnalysisExcel> excelWrapper = Wrappers.lambdaQuery();
												excelWrapper.eq(BankCashflowAnalysisExcel::getExcelNo, excelNo);
												excelService.update(analysisExcel, excelWrapper);

												analysisExcelOne.setRemark(errorMsg);
												errorExcelList.add(analysisExcelOne);
												continue;
											}
											JSONObject ecfJO = excelCashFlowJA.getJSONObject(0);

											List<JSONObject> mapList = excelCashFlowJA.toJavaList(JSONObject.class);
											String finalOrderNo = orderNo;
											String finalFlowType = flowType;
											List<BankCashflowExcelDetails> collect = mapList.stream().map(x -> {
												BankCashflowExcelDetails temp = new BankCashflowExcelDetails();
												temp.setOrderNumber(finalOrderNo);
												temp.setExcelNum(excelNo);
												temp.setCategoryType(String.valueOf(x.get("categoryType")));
												temp.setPurposeCategory(String.valueOf(x.get("purposeCategory")));
												temp.setTradeDate(String.valueOf(x.get("tradeDate")));
												//temp.setTradeDate(String.valueOf(temp.getTradeDate2()));
												temp.setCurrency(String.valueOf(x.get("currency")));
												temp.setBorrow(String.valueOf(x.get("borrow")));
												temp.setLend(String.valueOf(x.get("lend")));
												temp.setBalanceType(Integer.valueOf((Integer) x.get("balanceType")));
												temp.setTradeWay(String.valueOf(x.get("tradeWay")));
												temp.setTradeOrder(String.valueOf(x.get("tradeOrder")));
												temp.setMerchantOrder(String.valueOf(x.get("merchantOrder")));
												temp.setDealAccountName(String.valueOf(x.get("dealAccountName")));
												temp.setPurpose(String.valueOf(x.get("purpose")));
												temp.setOther(String.valueOf(x.get("other")));
												temp.setRemark(String.valueOf(x.get("remark")));
												temp.setType(Integer.valueOf((Integer) x.get("type")));
												temp.setStatus(Integer.valueOf((Integer) x.get("status")));
												temp.setDeptId(bankCashflowAnalysisDetail.getDeptId());
												temp.setUserId(bankCashflowAnalysisDetail.getUserId());
												temp.setCreateBy(bankCashflowAnalysisDetail.getUserId().intValue());
												temp.setFlowType(finalFlowType);

												return temp;
											}).collect(Collectors.toList());

											if (CollectionUtils.isNotEmpty(collect)) {
												//如果第一条时间>最后一条时间，说明倒序，需要排序
												String firstTradeStr = collect.get(0).getTradeDate();
												String endTradeStr = collect.get(collect.size() - 1).getTradeDate();
												Date firstTradeDate = DateUtil.parseDate(firstTradeStr);
												Date endTradeDate = DateUtil.parseDate(endTradeStr);
												if (firstTradeDate.after(endTradeDate)) {
													Collections.reverse(collect);
												}
											}
											//保存excel明细
											excelDetailsService.saveBatch(collect);

											//计算命中决策，并入库
											excelDetailsRuleService.calculateDetailRule(collect);

											//补充Excel对应关系
											BankCashflowAnalysisExcel analysisExcel = new BankCashflowAnalysisExcel();
											analysisExcel.setOrderNo(orderNo);
											analysisExcel.setExcelNo(excelNo);
											//analysisExcel.setAccountNo(ecfJO.getString("account"));
											//analysisExcel.setAccountName(ecfJO.getString("bankName"));
											analysisExcel.setFileName(analysisExcelOne.getFileName());
											analysisExcel.setCashflowStartTime(DateUtils.dateTime("yyyy-MM-dd HH:mm:ss", dataJO.getString("minTradeDate")));
											analysisExcel.setCashflowEndTime(DateUtils.dateTime("yyyy-MM-dd HH:mm:ss", dataJO.getString("maxTradeDate")));
											analysisExcel.setStatus(1);
											LambdaQueryWrapper<BankCashflowAnalysisExcel> excelWrapper = Wrappers.lambdaQuery();
											excelWrapper.eq(BankCashflowAnalysisExcel::getExcelNo, excelNo);
											excelService.update(analysisExcel, excelWrapper);
										}
										return null;
									}).collect(Collectors.toList());

							// 提交任务并等待完成
							List<Future<Void>> futures = innerExecutor.invokeAll(innerTasks);
							for (Future<Void> future : futures) {//调用 future.get() 会阻塞当前线程，直到该任务完成并返回结果,确保在执行 executor.shutdown() 方法之前，所有任务都已经完成
								future.get();
							}

							//关闭线程池
							innerExecutor.shutdown();

							if (errorMsg == null) {
								//补充流水数据
								BankCashflowAnalysisDetail updateAnalysisDetail = new BankCashflowAnalysisDetail();
								//updateAnalysisDetail.setCompanyName(ecfJO.getString("companyname"));
								//updateAnalysisDetail.setCashflowStartTime(DateUtils.dateTime("yyyy-MM-dd HH:mm:ss",dataJO.getString("minTradeDate")));
								//updateAnalysisDetail.setCashflowEndTime(DateUtils.dateTime("yyyy-MM-dd HH:mm:ss",dataJO.getString("maxTradeDate")));
								updateAnalysisDetail.setStatus(1);
								LambdaQueryWrapper<BankCashflowAnalysisDetail> queryWrapper = Wrappers.lambdaQuery();
								queryWrapper.eq(BankCashflowAnalysisDetail::getOrderNo, orderNo);
								analysisDetailService.update(updateAnalysisDetail, queryWrapper);

							} else {
								BankCashflowAnalysisDetail updateAnalysisDetail = new BankCashflowAnalysisDetail();
								updateAnalysisDetail.setStatus(4);
								updateAnalysisDetail.setRemark(detailErrorMsg);
								LambdaQueryWrapper<BankCashflowAnalysisDetail> queryWrapper = Wrappers.lambdaQuery();
								queryWrapper.eq(BankCashflowAnalysisDetail::getOrderNo, orderNo);
								analysisDetailService.update(updateAnalysisDetail, queryWrapper);
								//更新状态到risk-value reportHistory
								updateAnalysisDetail.setOrderNo(orderNo);
								analysisDetailService.reportChangeCallBack(updateAnalysisDetail, errorExcelList.get(0).getFileName());

								//异常推送飞书消息
								for (BankCashflowAnalysisExcel errorExcel : errorExcelList) {
									CommonUtil.sendBotMessage("【Excel解析异常】订单号：" + errorDetail.getOrderNo() + "，文件链接：" + errorExcel.getFileUrl() + "，错误消息：" + errorMsg);
								}
							}

						}
						return null;
					}).collect(Collectors.toList());

			// 提交任务并等待完成
			List<Future<Void>> futures = executor.invokeAll(tasks);
			for (Future<Void> future : futures) {//调用 future.get() 会阻塞当前线程，直到该任务完成并返回结果,确保在执行 executor.shutdown() 方法之前，所有任务都已经完成
				future.get();
			}

			// 关闭线程池
			executor.shutdown();

		}catch (Exception e){
			log.info("=====流水计算服务：计算出错{}===== \n", e);
			//更新状态为异常
			errorDetail.setStatus(4);
			errorDetail.setRemark(e.getMessage().length()>=255?e.getMessage().substring(0,254):e.getMessage());
			analysisDetailService.updateById(errorDetail);
			//更新状态到risk-value reportHistory
			analysisDetailService.reportChangeCallBack(errorDetail,errorExcelList.get(0).getFileName());
			//操作EXCL表数据
			currentExcel.setStatus(4);
			currentExcel.setRemark(e.getMessage().length()>=255?e.getMessage().substring(0,254):e.getMessage());
			excelService.updateById(currentExcel);

			//异常推送飞书消息
			for(BankCashflowAnalysisExcel errorExcel : errorExcelList) {
				CommonUtil.sendBotMessage("【Excel解析异常】订单号：" + errorDetail.getOrderNo() + "，文件链接：" + errorExcel.getFileUrl() + "，错误消息：" + errorExcel.getRemark());
			}
			return ReturnT.FAIL;
		}
		return ReturnT.SUCCESS;
	}

}
