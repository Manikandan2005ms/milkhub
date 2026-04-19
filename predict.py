import joblib
import pandas as pd
import numpy as np
import os
from datetime import datetime

class MilkAI:
    def __init__(self):
        self.yield_model = self._load_model('yield_model.pkl')
        self.quality_model = self._load_model('quality_model.pkl')
        self.fraud_model = self._load_model('fraud_model.pkl')

    def _load_model(self, filename):
        if os.path.exists(filename):
            try:
                return joblib.load(filename)
            except:
                return None
        return None

    def _clean_val(self, val, default=0.0):
        """Standardize numeric values from strings like '2.00%'."""
        if val is None or val == "": return float(default)
        if isinstance(val, (int, float)): return float(val)
        try:
            s = str(val).replace('%', '').replace('₹', '').replace(',', '').strip()
            return float(s)
        except:
            return float(default)

    def predict_tomorrow(self, history_df):
        """Predict tomorrow's milk yield based on 7-day history trend."""
        if history_df.empty:
            return 0.0
        
        # Simple weighted moving average or trend extrapolation
        litres = [self._clean_val(x) for x in history_df['litres'].tolist()]
        if len(litres) < 3:
            return round(float(np.mean(litres)), 2)
        
        # Calculate trend (latest weight > older weight)
        weights = np.linspace(0.5, 1.0, len(litres))
        weighted_avg = np.average(litres, weights=weights)
        
        # Add a slight variation based on recent growth/drop
        recent_change = (litres[-1] - litres[0]) / len(litres)
        prediction = float(weighted_avg) + (float(recent_change) * 0.5)
        
        return round(float(max(0, prediction)), 2)

    def calculate_quality_score(self, fat, snf, water_percent):
        """Calculate a quality score from 0 to 100."""
        score = 0
        
        # Fat: 40 points (Base on 4.5% as ideal)
        f = self._clean_val(fat)
        score += min(40, (f / 4.5) * 40)
        
        # SNF: 30 points (Base on 8.5% as ideal)
        s = self._clean_val(snf, 8.5)
        score += min(30, (s / 8.5) * 30)
        
        # Water Mixing: 30 points (Deduct for water)
        w = self._clean_val(water_percent)
        water_penalty = min(30, (w / 10) * 30) # 10% water = 30 point penalty
        score += (30 - water_penalty)
        
        return int(max(0, min(100, score)))

    def detect_fraud(self, litres, fat, ph, temperature, water_percent=0, history=None):
        risk_level = "Low"
        reasons = []

        # Ensure all types are float for comparison/math
        l_val = self._clean_val(litres)
        f_val = self._clean_val(fat)
        p_val = self._clean_val(ph, 6.6)
        t_val = self._clean_val(temperature, 35.0)
        w_val = self._clean_val(water_percent)

        # 1. Rule-based checks
        if 6.0 > p_val or p_val > 7.5: 
            risk_level = "High"
            reasons.append("pH Anomaly")
        if t_val > 40:
            risk_level = "High"
            reasons.append("Temperature High")
        if w_val > 10:
            risk_level = "High"
            reasons.append("Water Mixing")
        if f_val < 2.0:
            risk_level = "Medium"
            reasons.append("Low Fat")

        # 2. Consistency check with history
        try:
            if history is not None and hasattr(history, 'empty') and not history.empty:
                # Use engine's own cleaning logic for the mean
                litres_series = history['litres'].apply(self._clean_val)
                avg_litres = float(litres_series.mean())
                if pd.notnull(avg_litres) and l_val > avg_litres * 1.5:
                    risk_level = "High"
                    reasons.append("Abnormal Volume Spike")
        except:
            pass # Keep it low risk if history check fails

        return risk_level

    def get_production_trend(self, history_df):
        try:
            if history_df is None or (hasattr(history_df, 'empty') and history_df.empty) or len(history_df) < 3:
                return "Stable"
            
            litres = [self._clean_val(x) for x in history_df['litres'].tolist()]
            recent_avg = np.mean(litres[-3:])
            older_avg = np.mean(litres[:-3]) if len(litres) > 3 else litres[0]
            
            diff = (recent_avg - older_avg) / (older_avg or 1)
            if diff > 0.05: return "Increasing"
            if diff < -0.05: return "Decreasing"
            return "Stable"
        except:
            return "Stable"

    def get_confidence_score(self, history_len):
        if history_len > 15: return "High"
        if history_len > 7: return "Medium"
        return "Low"

    def get_smart_recommendation(self, data, quality):
        """Generate dynamic recommendations based on specific row data."""
        recs = []
        fat = self._clean_val(data.get('fat'))
        water = self._clean_val(data.get('water_percent'))
        ph = self._clean_val(data.get('ph'), 6.6)
        temp = self._clean_val(data.get('temperature'), 35.0)

        # 1. Water Rules
        if water > 20:
            recs.append("High water mixing detected. Reject batch and inspect source.")
        elif water > 10:
            recs.append("Possible adulteration. Recheck sample and warn farmer.")
        elif water > 5:
            recs.append("Minor dilution suspected. Monitor next collection.")

        # 2. Fat Rules
        if fat < 2:
            recs.append("Very low fat. Improve feed quality and cow health.")
        elif 2 <= fat < 3:
            recs.append("Low fat milk. Add balanced nutrition.")

        # 3. pH Rules
        if ph < 6.0:
            recs.append("Acidity issue detected. Use immediately or discard.")
        elif ph > 7.0:
            recs.append("Abnormal pH. Re-test sample.")

        # 4. Temperature Rules
        if temp > 35:
            recs.append("Cool milk immediately to prevent spoilage.")

        # 5. Quality specific
        if quality == "Good" and water <= 3:
            recs.append("Milk quality good. Continue same practice.")
        elif quality == "Excellent":
            recs.append("Premium quality milk. Eligible for bonus.")

        if not recs:
            recs.append("Maintain consistency and check periodic health.")

        return " | ".join(recs)

    def get_personalized_recommendation(self, score, trend, data):
        recs = []
        fat = self._clean_val(data.get('fat'))
        water = self._clean_val(data.get('water_percent'))
        
        if water > 5:
            recs.append("Reduce water mixing to improve grade.")
        if fat < 3.5:
            recs.append("Improve feed quality (add oil cakes/green fodder).")
        if trend == "Decreasing":
            recs.append("Check cow health; production is dropping.")
        if score > 85:
            recs.append("Excellent consistency! Maintain current feeding schedule.")
        
        if not recs:
            recs.append("Maintain consistency and check periodic health.")
            
        return " | ".join(recs[:2])

ai_engine = MilkAI()
