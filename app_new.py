from flask import Flask, request, jsonify, render_template
import os
import sys

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from agent import FinancialRiskAgent

app = Flask(__name__)

# 初始化FinancialRiskAgent
agent = FinancialRiskAgent()

@app.route('/')
def index():
    return render_template('index_new.html')

@app.route('/analyze', methods=['POST'])
def analyze():
    try:
        # 获取股票名称或代码
        stock_input = request.form['stock_input']
        
        # 添加空字符串检查
        if not stock_input or stock_input.strip() == '':
            return jsonify({'success': False, 'error': '股票名称或代码不能为空'})
        
        # 转换为股票代码
        code = agent.stock_name_to_code(stock_input)
        
        # 获取股票基本信息
        basic_info = agent.get_stock_basic_info(code)
        
        # 获取行业信息
        industry_info = agent.get_industry_info(code)
        
        # 计算综合风险分析
        comprehensive_risk = agent.calculate_comprehensive_risk(code)
        
        # 计算各个分析结果
        volatility_analysis = agent.calculate_volatility_analysis(code)
        trend_analysis = agent.calculate_trend_analysis(code)
        turnover_analysis = agent.calculate_turnover_analysis(code)
        valuation_analysis = agent.calculate_valuation_analysis(code)
        historical_valuation = agent.calculate_historical_valuation_analysis(code)
        financial_health = agent.calculate_financial_health_analysis(code)
        rsi_analysis = agent.calculate_rsi_analysis(code)
        
        # 构建响应数据，确保字段名称与前端匹配
        response = {
            'success': True,
            'code': code,
            'basic_info': {
                'code': code,
                'name': basic_info.get('name', code),
                'current_price': basic_info.get('current_price', 0),
                'change_percent': basic_info.get('change_percent', 0)
            },
            'comprehensive_risk': {
                'risk_level': comprehensive_risk.get('risk_level', '未知'),
                'final_score': comprehensive_risk.get('comprehensive_score', 0),
                'industry': industry_info.get('industry', '未知'),
                'sw_industry': industry_info.get('sw_industry', '未知')
            },
            'volatility_analysis': {
                'volatility_score': volatility_analysis.get('volatility_score', 0),
                'annualized_volatility': volatility_analysis.get('absolute_volatility', None),
                'volatility_level': volatility_analysis.get('volatility_level', '未知')
            },
            'trend_analysis': {
                'trend_score': trend_analysis.get('final_score', 0),
                'trend_status': trend_analysis.get('trend_status', '未知'),
                'trend_strength': trend_analysis.get('trend_strength', '未知')
            },
            'turnover_analysis': {
                'turnover_score': turnover_analysis.get('score', 0),
                'actual_turnover': turnover_analysis.get('actual_turnover', 0),
                'benchmark_turnover': turnover_analysis.get('benchmark_turnover', 0)
            },
            'valuation_analysis': valuation_analysis,
            'historical_valuation': {
                'final_score': historical_valuation.get('final_score', 0),
                'current_valuation': historical_valuation.get('current_valuation', {}),
                'percentiles': historical_valuation.get('percentiles', {})
            },
            'financial_health': {
                'final_score': financial_health.get('total_score', 0),
                'profitability_score': financial_health.get('profitability_score', 0),
                'cashflow_score': financial_health.get('cashflow_score', 0),
                'solvency_score': financial_health.get('solvency_score', 0)
            },
            'rsi_analysis': {
                'rsi_score': rsi_analysis.get('final_score', 0),
                'rsi_value': rsi_analysis.get('current_rsi', None),
                'rsi_status': rsi_analysis.get('rsi_status', '未知')
            }
        }
        
        return jsonify(response)
    except Exception as e:
        # 添加详细的错误日志
        import traceback
        error_msg = f"分析失败: {str(e)}"
        error_detail = traceback.format_exc()
        print(f"[ERROR] {error_msg}")
        print(f"[ERROR DETAIL] {error_detail}")
        return jsonify({'success': False, 'error': error_msg, 'detail': error_detail})

@app.route('/generate_report', methods=['POST'])
def generate_report():
    try:
        # 获取股票名称或代码
        stock_input = request.form['stock_input']
        
        # 添加空字符串检查
        if not stock_input or stock_input.strip() == '':
            return jsonify({'success': False, 'error': '股票名称或代码不能为空'})
        
        # 转换为股票代码
        code = agent.stock_name_to_code(stock_input)
        
        # 生成报告
        comprehensive_risk = agent.calculate_comprehensive_risk(code)
        
        # 获取报告路径
        stock_name = comprehensive_risk.get('stock_name', code)
        report_filename = f"{stock_name}_{code}_综合风险分析报告.md"
        report_path = os.path.join('reports', report_filename)
        
        return jsonify({'success': True, 'report_path': report_path})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    # 确保reports目录存在
    if not os.path.exists('reports'):
        os.makedirs('reports')
    
    app.run(debug=True, host='0.0.0.0', port=5000)
