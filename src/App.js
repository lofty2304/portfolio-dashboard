import React, { useState, useEffect } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, BarChart, Bar, Legend } from 'recharts';
import { DollarSign, TrendingUp, TrendingDown, RefreshCw, AlertTriangle, CheckCircle, Clock, Calendar, Target, Globe, PieChart as PieChartIcon, BarChart2, Zap, LayoutDashboard } from 'lucide-react';

// Tailwind CSS is assumed to be available in the environment.
// No explicit import needed for Tailwind classes in React components.

// Helper function to format currency
const formatCurrency = (value) => {
  if (value === undefined || value === null) return 'N/A';
  return `₹${parseFloat(value).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
};

// Helper function to format percentage
const formatPercentage = (value) => {
  if (value === undefined || value === null) return 'N/A';
  return `${(parseFloat(value) * 100).toFixed(2)}%`;
};

// Colors for charts
const COLORS = ['#8884d8', '#82ca9d', '#ffc658', '#ff7300', '#00c49f', '#ffbb28', '#a4de6c', '#d0ed57', '#83a6ed', '#8dd1e1'];

// Mock Data Loaders (In a real deployment, these would fetch from your backend or cloud storage)
// For now, we'll use simplified structures based on your CSV snippets.

// Mock Overview Dashboard Data
const mockOverviewData = [
  { metric: "Total Portfolio Value", currentValue: 134193.54, targetValue: 200000, status: "🟡 Growing", notes: "67% of target achieved" },
  { metric: "Total Invested Amount", currentValue: 125273.59, targetValue: 180000, status: "🟢 On Track", notes: "Investment pace good" },
  { metric: "Total Gains", currentValue: 8919.95, targetValue: 20000, status: "🟢 Positive", notes: "7.12% absolute return" },
  { metric: "Current Weighted CAGR", currentValue: 0.0665, targetValue: 0.12, status: "🔴 Below Target", notes: "Needs strategic rebalancing" },
  { metric: "Equity Allocation", currentValue: 0.549, targetValue: 0.5, status: "🟢 Optimal", notes: "Slightly overweight" },
  { metric: "International Exposure", currentValue: 0.007, targetValue: 0.35, status: "🔴 Very Low", notes: "Critical rebalancing needed" },
  { metric: "Gold/Commodity Allocation", currentValue: 0.14, targetValue: 0.08, status: "🟡 High", notes: "Consider reducing" },
  { metric: "Conservative Allocation", currentValue: 0.212, targetValue: 0.05, status: "🔴 Too High", notes: "Major reduction needed" },
  { metric: "Cash Allocation", currentValue: 0.092, targetValue: 0.02, status: "🔴 Too High", notes: "Deploy excess cash" },
  { metric: "Risk Score (1-10)", currentValue: 6.5, targetValue: 7, status: "🟡 Moderate", notes: "Increase for higher returns" },
];

// Mock Technical Indicators Data
const mockTechnicalIndicators = [
  { indicator: "Nifty 50 Level", value: 24800, signal: "Neutral", trend: "Sideways", action: "Continue SIP", supportResistance: "Support: 25,000" },
  { indicator: "VIX Volatility", value: 14, signal: "Low Volatility", trend: "Stable", action: "Favorable for equity", supportResistance: "Resistance: 20" },
  { indicator: "EUR/INR Rate", value: 100.5996, signal: "Near Peak", trend: "Uptrend", action: "Avoid EU lumpsum", supportResistance: "Resistance: 102" },
  { indicator: "USD/INR Rate", value: 83.42, signal: "Stable", trend: "Stable", action: "Neutral for US", supportResistance: "Support: 83" },
  { indicator: "Gold Price (MCX)", value: 72500, signal: "Consolidation", trend: "Sideways", action: "Book partial profits", supportResistance: "Support: ₹71,000" },
  { indicator: "Nifty P/E Ratio", value: 21.5, signal: "Expensive", trend: "High", action: "Stagger investments", supportResistance: "High Zone: >22" },
  { indicator: "FII Flows (Monthly)", value: 15000, signal: "Positive", trend: "Improving", action: "Maintain equity allocation", supportResistance: "Positive Zone: >₹10,000" },
  { indicator: "DII Flows (Monthly)", value: 18000, signal: "Strong", trend: "Consistent", action: "Positive for domestic", supportResistance: "Positive Zone: >₹15,000" },
];

// Mock Historical Performance Data (simplified for chart demo)
const mockHistoricalPerformance = [
  { date: '2024-01-01', portfolioValue: 100000, niftyIndexed: 100, goldIndexed: 100 },
  { date: '2024-02-01', portfolioValue: 102000, niftyIndexed: 101, goldIndexed: 100.5 },
  { date: '2024-03-01', portfolioValue: 105000, niftyIndexed: 103, goldIndexed: 101 },
  { date: '2024-04-01', portfolioValue: 107000, niftyIndexed: 102.5, goldIndexed: 102 },
  { date: '2024-05-01', portfolioValue: 109000, niftyIndexed: 104, goldIndexed: 101.5 },
  { date: '2024-06-01', portfolioValue: 112000, niftyIndexed: 105.5, goldIndexed: 102.5 },
  { date: '2024-07-01', portfolioValue: 115000, niftyIndexed: 107, goldIndexed: 103 },
];

function App() {
  const [activeTab, setActiveTab] = useState('overview');
  const [overviewData, setOverviewData] = useState([]);
  const [technicalIndicators, setTechnicalIndicators] = useState([]);
  const [historicalPerformance, setHistoricalPerformance] = useState([]);

  useEffect(() => {
    // In a real app, you'd fetch this data from your backend API or cloud storage
    // For now, we're using mock data
    setOverviewData(mockOverviewData);
    setTechnicalIndicators(mockTechnicalIndicators);
    setHistoricalPerformance(mockHistoricalPerformance.map(d => ({
      ...d,
      date: new Date(d.date).toLocaleDateString('en-IN', { month: 'short', year: '2-digit' })
    })));
  }, []);

  // Process data for charts
  const allocationData = overviewData.filter(item => item.metric.includes('Allocation')).map(item => ({
    name: item.metric.replace(' Allocation', ''),
    value: item.currentValue,
  }));

  const pieChartData = [
    { name: 'Equity', value: overviewData.find(d => d.metric === 'Equity Allocation')?.currentValue || 0 },
    { name: 'International', value: overviewData.find(d => d.metric === 'International Exposure')?.currentValue || 0 },
    { name: 'Gold/Commodity', value: overviewData.find(d => d.metric === 'Gold/Commodity Allocation')?.currentValue || 0 },
    { name: 'Conservative', value: overviewData.find(d => d.metric === 'Conservative Allocation')?.currentValue || 0 },
    { name: 'Cash', value: overviewData.find(d => d.metric === 'Cash Allocation')?.currentValue || 0 },
  ].filter(item => item.value > 0); // Filter out zero values for better visualization

  const getStatusIcon = (status) => {
    if (status.includes('🟢')) return <CheckCircle className="text-green-500" size={18} />;
    if (status.includes('🟡')) return <AlertTriangle className="text-yellow-500" size={18} />;
    if (status.includes('🔴')) return <TrendingDown className="text-red-500" size={18} />;
    return null;
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 to-gray-800 text-gray-100 font-inter p-4 sm:p-8">
      <header className="mb-8 text-center">
        <h1 className="text-5xl font-extrabold mb-2 text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-purple-600">
          QuantEdge Portfolio Dashboard
        </h1>
        <p className="text-lg text-gray-400">Your AI-Powered Hedge Fund Manager</p>
      </header>

      <nav className="flex justify-center mb-8">
        <button
          className={`px-6 py-3 rounded-full text-lg font-semibold transition-all duration-300 mx-2 ${
            activeTab === 'overview' ? 'bg-purple-600 text-white shadow-lg' : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
          }`}
          onClick={() => setActiveTab('overview')}
        >
          <LayoutDashboard className="inline-block mr-2" size={20} /> Overview
        </button>
        <button
          className={`px-6 py-3 rounded-full text-lg font-semibold transition-all duration-300 mx-2 ${
            activeTab === 'indicators' ? 'bg-purple-600 text-white shadow-lg' : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
          }`}
          onClick={() => setActiveTab('indicators')}
        >
          <Zap className="inline-block mr-2" size={20} /> Indicators
        </button>
        <button
          className={`px-6 py-3 rounded-full text-lg font-semibold transition-all duration-300 mx-2 ${
            activeTab === 'performance' ? 'bg-purple-600 text-white shadow-lg' : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
          }`}
          onClick={() => setActiveTab('performance')}
        >
          <TrendingUp className="inline-block mr-2" size={20} /> Performance
        </button>
      </nav>

      <main className="max-w-7xl mx-auto bg-gray-800 rounded-3xl shadow-2xl p-6 sm:p-10 border border-gray-700">
        {activeTab === 'overview' && (
          <section>
            <h2 className="text-4xl font-bold mb-8 text-center text-blue-300">Portfolio Overview</h2>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-10">
              {overviewData.filter(item => item.metric.includes('Value') || item.metric.includes('Amount') || item.metric.includes('Gains')).map((item, index) => (
                <div key={index} className="bg-gray-700 p-6 rounded-2xl shadow-lg flex items-center justify-between border border-gray-600 transform hover:scale-105 transition-transform duration-200">
                  <div>
                    <p className="text-lg text-gray-400">{item.metric}</p>
                    <p className="text-3xl font-bold text-white mt-1">
                      {item.metric.includes('Value') || item.metric.includes('Amount') || item.metric.includes('Gains')
                        ? formatCurrency(item.currentValue)
                        : formatPercentage(item.currentValue)}
                    </p>
                  </div>
                  <div className="flex items-center space-x-2">
                    {getStatusIcon(item.status)}
                    <span className={`text-sm font-medium ${item.status.includes('🟢') ? 'text-green-400' : item.status.includes('🟡') ? 'text-yellow-400' : 'text-red-400'}`}>
                      {item.status.replace(/[^a-zA-Z\s]/g, '')}
                    </span>
                  </div>
                </div>
              ))}
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
              <div className="bg-gray-700 p-6 rounded-2xl shadow-lg border border-gray-600">
                <h3 className="text-2xl font-bold mb-4 text-blue-300 flex items-center">
                  <PieChartIcon size={24} className="mr-2" /> Allocation by Category
                </h3>
                <ResponsiveContainer width="100%" height={300}>
                  <PieChart>
                    <Pie
                      data={pieChartData}
                      cx="50%"
                      cy="50%"
                      outerRadius={100}
                      fill="#8884d8"
                      dataKey="value"
                      labelLine={false}
                      label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`}
                    >
                      {pieChartData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(value) => formatPercentage(value)} />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              </div>

              <div className="bg-gray-700 p-6 rounded-2xl shadow-lg border border-gray-600">
                <h3 className="text-2xl font-bold mb-4 text-blue-300 flex items-center">
                  <BarChart2 size={24} className="mr-2" /> Allocation Status
                </h3>
                <div className="overflow-x-auto">
                  <table className="min-w-full bg-gray-700 rounded-lg">
                    <thead>
                      <tr className="bg-gray-600 text-gray-300 uppercase text-sm leading-normal">
                        <th className="py-3 px-6 text-left">Metric</th>
                        <th className="py-3 px-6 text-center">Current</th>
                        <th className="py-3 px-6 text-center">Target</th>
                        <th className="py-3 px-6 text-center">Status</th>
                      </tr>
                    </thead>
                    <tbody className="text-gray-200 text-sm font-light">
                      {overviewData.filter(item => item.metric.includes('Allocation') || item.metric.includes('CAGR') || item.metric.includes('Risk')).map((item, index) => (
                        <tr key={index} className="border-b border-gray-600 hover:bg-gray-600">
                          <td className="py-3 px-6 text-left whitespace-nowrap">{item.metric}</td>
                          <td className="py-3 px-6 text-center">
                            {item.metric.includes('CAGR') || item.metric.includes('Allocation')
                              ? formatPercentage(item.currentValue)
                              : item.currentValue}
                          </td>
                          <td className="py-3 px-6 text-center">
                            {item.metric.includes('CAGR') || item.metric.includes('Allocation')
                              ? formatPercentage(item.targetValue)
                              : item.targetValue}
                          </td>
                          <td className="py-3 px-6 text-center flex items-center justify-center">
                            {getStatusIcon(item.status)}
                            <span className={`ml-2 ${item.status.includes('🟢') ? 'text-green-400' : item.status.includes('🟡') ? 'text-yellow-400' : 'text-red-400'}`}>
                              {item.status.replace(/[^a-zA-Z\s]/g, '')}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </section>
        )}

        {activeTab === 'indicators' && (
          <section>
            <h2 className="text-4xl font-bold mb-8 text-center text-blue-300">Technical Indicators & Market Sentiment</h2>
            <div className="overflow-x-auto bg-gray-700 rounded-2xl shadow-lg border border-gray-600 p-4">
              <table className="min-w-full bg-gray-700 rounded-lg">
                <thead>
                  <tr className="bg-gray-600 text-gray-300 uppercase text-sm leading-normal">
                    <th className="py-3 px-6 text-left">Indicator</th>
                    <th className="py-3 px-6 text-center">Current Value</th>
                    <th className="py-3 px-6 text-center">Signal</th>
                    <th className="py-3 px-6 text-center">Trend</th>
                    <th className="py-3 px-6 text-left">Action</th>
                    <th className="py-3 px-6 text-left">Support/Resistance</th>
                  </tr>
                </thead>
                <tbody className="text-gray-200 text-sm font-light">
                  {technicalIndicators.map((item, index) => (
                    <tr key={index} className="border-b border-gray-600 hover:bg-gray-600">
                      <td className="py-3 px-6 text-left whitespace-nowrap">{item.indicator}</td>
                      <td className="py-3 px-6 text-center">
                        {typeof item.value === 'number' && item.indicator.includes('Price') ? formatCurrency(item.value) : item.value}
                        {typeof item.value === 'number' && item.indicator.includes('Flows') ? `₹${item.value.toLocaleString('en-IN')} Cr` : ''}
                      </td>
                      <td className="py-3 px-6 text-center">{item.signal}</td>
                      <td className="py-3 px-6 text-center">{item.trend}</td>
                      <td className="py-3 px-6 text-left">{item.action}</td>
                      <td className="py-3 px-6 text-left">{item.supportResistance}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="text-gray-400 text-sm mt-6 text-center">
              <RefreshCw className="inline-block mr-1" size={16} /> Data as of 2025-07-21 (Mock Data)
            </p>
          </section>
        )}

        {activeTab === 'performance' && (
          <section>
            <h2 className="text-4xl font-bold mb-8 text-center text-blue-300">Performance Analytics</h2>

            <div className="bg-gray-700 p-6 rounded-2xl shadow-lg border border-gray-600 mb-8">
              <h3 className="text-2xl font-bold mb-4 text-blue-300 flex items-center">
                <TrendingUp size={24} className="mr-2" /> Indexed Performance: Portfolio vs. Benchmarks
              </h3>
              <ResponsiveContainer width="100%" height={400}>
                <AreaChart
                  data={historicalPerformance}
                  margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#4a5568" />
                  <XAxis dataKey="date" stroke="#cbd5e0" />
                  <YAxis stroke="#cbd5e0" />
                  <Tooltip />
                  <Area type="monotone" dataKey="portfolioValue" stroke="#8884d8" fillOpacity={1} fill="url(#colorPortfolio)" name="Your Portfolio" />
                  <Area type="monotone" dataKey="niftyIndexed" stroke="#82ca9d" fillOpacity={1} fill="url(#colorNifty)" name="Nifty 50 (Indexed)" />
                  <Area type="monotone" dataKey="goldIndexed" stroke="#ffc658" fillOpacity={1} fill="url(#colorGold)" name="Gold (Indexed)" />
                  <defs>
                    <linearGradient id="colorPortfolio" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#8884d8" stopOpacity={0.8}/>
                      <stop offset="95%" stopColor="#8884d8" stopOpacity={0}/>
                    </linearGradient>
                    <linearGradient id="colorNifty" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#82ca9d" stopOpacity={0.8}/>
                      <stop offset="95%" stopColor="#82ca9d" stopOpacity={0}/>
                    </linearGradient>
                    <linearGradient id="colorGold" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#ffc658" stopOpacity={0.8}/>
                      <stop offset="95%" stopColor="#ffc658" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                </AreaChart>
              </ResponsiveContainer>
              <p className="text-gray-400 text-sm mt-4 text-center">
                This chart shows the relative growth of your portfolio compared to Nifty 50 and Gold, indexed to 100 at the start.
              </p>
            </div>

            <div className="bg-gray-700 p-6 rounded-2xl shadow-lg border border-gray-600">
              <h3 className="text-2xl font-bold mb-4 text-blue-300 flex items-center">
                <BarChart2 size={24} className="mr-2" /> Allocation Breakdown
              </h3>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={allocationData} layout="vertical" margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#4a5568" />
                  <XAxis type="number" stroke="#cbd5e0" tickFormatter={formatPercentage} />
                  <YAxis type="category" dataKey="name" stroke="#cbd5e0" />
                  <Tooltip formatter={(value) => formatPercentage(value)} />
                  <Legend />
                  <Bar dataKey="value" fill="#8884d8" name="Current Allocation" />
                </BarChart>
              </ResponsiveContainer>
              <p className="text-gray-400 text-sm mt-4 text-center">
                Current allocation across different asset classes.
              </p>
            </div>
          </section>
        )}
      </main>

      <footer className="mt-10 text-center text-gray-500 text-sm">
        <p>&copy; {new Date().getFullYear()} QuantEdge. All rights reserved.</p>
        <p>Disclaimer: Investment decisions should be made with professional advice.</p>
      </footer>
    </div>
  );
}

export default App;
